use crate::{
    client::{hc_connection, Client, Data, Interface, LoginConfigHandler},
    port_forward_mux::{
        charge, close_msg, effective_window, open_msg, run_channel, FrameSink, Inbound, RecvWindow,
        SendCredit, Tunnel, TunnelHandle, CHANNEL_WINDOW, INITIAL_WINDOW, MAX_CHANNELS,
    },
};
use async_trait::async_trait;
use base::message_proto::*;
use hbb_common::{
    allow_err,
    config::{Config, LocalConfig, PeerConfig, READ_TIMEOUT},
    futures::{SinkExt, StreamExt},
    log,
    protobuf::Message as _,
    rendezvous_proto::ConnType,
    timeout,
    tokio::{
        self,
        net::{TcpListener, TcpStream},
        sync::{mpsc, watch},
        time::{sleep, Duration},
    },
    tokio_util::codec::{BytesCodec, Framed},
    ResultType, Stream,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc, Mutex, RwLock,
    },
};

const OPTION_LAN_PEER_IPS: &str = "lan_peer_ips";
const OPTION_LAN_ENABLED_PEERS: &str = "lan_enabled_peers";
const OPTION_LAN_PEER_MODES: &str = "lan_peer_modes";

const DEFAULT_LAN_PORTS: &[u16] = &[
    21, 22, 80, 139, 443, 445, 1433, 1521, 1883, 2049, 2375, 3000, 3001, 3306, 3389, 4000, 4200,
    5000, 5001, 5173, 5174, 5432, 5500, 5672, 5900, 5985, 5986, 6000, 6379, 6443, 7000, 7001,
    7860, 8000, 8001, 8008, 8080, 8081, 8088, 8090, 8123, 8443, 8888, 9000, 9001, 9090, 9092,
    9100, 9200, 9300, 9418, 10000, 11211, 11434, 15672, 25565, 27015, 27017,
];

pub const HOT_UPDATE_VERSION: &str = "1.5.1";
pub const HOT_UPDATE_BUILD: u64 = 74;
pub const DEFAULT_HOT_UPDATE_MANIFEST_URL: &str = "http://124.222.82.53/downloads/version.json";
pub const DEFAULT_HOTPATCH_MACOS_URL: &str =
    "http://124.222.82.53/downloads/hotpatch-macos-arm64.tar.gz";

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct VersionManifest {
    pub version: String,
    pub build: u64,
    #[serde(default)]
    pub hotpatch_macos_arm64_url: String,
    #[serde(default)]
    pub dmg_macos_arm64_url: String,
    #[serde(default)]
    pub sha256: String,
    #[serde(default)]
    pub notes: String,
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct HotUpdateStatus {
    pub local_version: String,
    pub local_build: u64,
    pub remote_version: String,
    pub remote_build: u64,
    pub state: String, // "idle" | "checking" | "downloading" | "applying" | "publishing" | "syncing" | "up_to_date" | "error"
    pub message: String,
    pub synced_peers: usize,
}

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct PeerLanState {
    pub id: String,
    pub ip: String,
    pub enabled: bool,
    pub status: String, // "off" | "connecting" | "connected" | "need_password" | "error"
    pub error: String,
    pub has_password: bool,
    #[serde(default)]
    pub remote_build: u64,
    #[serde(default)]
    pub remote_updating: bool,
    #[serde(default)]
    pub peer_mode: String, // "unknown" | "custom" | "legacy"
}

#[derive(Clone, Serialize, Deserialize, Default)]
struct CachedPeerMode {
    mode: String,
    build: u64,
}

struct ActivePeerWorker {
    stop_tx: watch::Sender<bool>,
}

static FORCE_UPDATE_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static LAST_APPLIED_FORCE_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static TARGET_UPDATE_BUILD: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(HOT_UPDATE_BUILD);
static IS_HOT_UPDATING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static PF_SUDO_CHECKED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

lazy_static::lazy_static! {
    static ref LAN_STATES: Mutex<HashMap<String, PeerLanState>> = Mutex::new(HashMap::new());
    static ref ACTIVE_WORKERS: Mutex<HashMap<String, ActivePeerWorker>> = Mutex::new(HashMap::new());
    static ref ACTIVE_TUNNELS: Mutex<HashMap<String, Arc<TunnelHandle>>> = Mutex::new(HashMap::new());
    static ref ALIAS_MOUNTED: Mutex<HashSet<String>> = Mutex::new(HashSet::new());
    static ref MACOS_PF_PORTS: Mutex<HashMap<(String, u16), Option<u16>>> = Mutex::new(HashMap::new());
    #[cfg(target_os = "linux")]
    static ref LINUX_NAT_PORTS: Mutex<HashMap<(String, u16), u16>> = Mutex::new(HashMap::new());
    static ref PROBED_PEERS: Mutex<HashSet<String>> = Mutex::new(HashSet::new());
    static ref INIT_ONCE: std::sync::Once = std::sync::Once::new();
    static ref HOT_UPDATE_LOOP_ONCE: std::sync::Once = std::sync::Once::new();
    static ref HOT_UPDATE_STATUS: Mutex<HotUpdateStatus> = Mutex::new(HotUpdateStatus {
        local_version: HOT_UPDATE_VERSION.to_owned(),
        local_build: HOT_UPDATE_BUILD,
        remote_version: HOT_UPDATE_VERSION.to_owned(),
        remote_build: HOT_UPDATE_BUILD,
        state: "idle".to_owned(),
        message: "自动热更新已就绪".to_owned(),
        synced_peers: 0,
    });
}

pub fn clean_peer_id(peer_id: &str) -> String {
    peer_id
        .split('@')
        .next()
        .unwrap_or(peer_id)
        .trim_end_matches("/r")
        .trim()
        .to_owned()
}

pub fn deterministic_lan_ip(peer_id: &str) -> (u8, u8) {
    let clean = clean_peer_id(peer_id);
    let mut hash: u32 = 0x811c9dc5;
    for b in clean.as_bytes() {
        hash ^= *b as u32;
        hash = hash.wrapping_mul(0x01000193);
    }
    let x = ((hash >> 8) % 250) as u8 + 2;
    let y = (hash % 250) as u8 + 2;
    (x, y)
}

fn load_ip_map() -> HashMap<String, String> {
    let raw = LocalConfig::get_option(OPTION_LAN_PEER_IPS);
    if raw.is_empty() {
        return HashMap::new();
    }
    serde_json::from_str(&raw).unwrap_or_default()
}

fn save_ip_map(map: &HashMap<String, String>) {
    if let Ok(s) = serde_json::to_string(map) {
        LocalConfig::set_option(OPTION_LAN_PEER_IPS.to_owned(), s);
    }
}

fn load_enabled_peers() -> HashSet<String> {
    let raw = LocalConfig::get_option(OPTION_LAN_ENABLED_PEERS);
    if raw.is_empty() {
        return HashSet::new();
    }
    serde_json::from_str(&raw).unwrap_or_default()
}

fn save_enabled_peers(set: &HashSet<String>) {
    if let Ok(s) = serde_json::to_string(set) {
        LocalConfig::set_option(OPTION_LAN_ENABLED_PEERS.to_owned(), s);
    }
}

fn load_peer_modes() -> HashMap<String, CachedPeerMode> {
    let raw = LocalConfig::get_option(OPTION_LAN_PEER_MODES);
    if raw.is_empty() {
        return HashMap::new();
    }
    serde_json::from_str(&raw).unwrap_or_default()
}

fn save_peer_mode(clean_id: &str, mode: &str, build: u64) {
    let mut map = load_peer_modes();
    let entry = map.entry(clean_id.to_owned()).or_default();
    entry.mode = mode.to_owned();
    if build > 0 {
        entry.build = build;
    } else if mode == "legacy" {
        entry.build = 0;
    }
    if let Ok(s) = serde_json::to_string(&map) {
        LocalConfig::set_option(OPTION_LAN_PEER_MODES.to_owned(), s);
    }
    let mut states = LAN_STATES.lock().unwrap();
    if let Some(st) = states.get_mut(clean_id) {
        st.peer_mode = mode.to_owned();
        if build > 0 {
            st.remote_build = build;
        } else if mode == "legacy" {
            st.remote_build = 0;
        }
    }
}

fn infer_initial_peer_mode(peer_id: &str, clean_id: &str) -> (String, u64) {
    let modes = load_peer_modes();
    if let Some(cached) = modes.get(clean_id) {
        if !cached.mode.is_empty() {
            return (cached.mode.clone(), cached.build);
        }
    }
    let cfg = PeerConfig::load(clean_id);
    let platform = if !cfg.info.platform.is_empty() {
        cfg.info.platform
    } else {
        PeerConfig::load(peer_id).info.platform
    };
    let p_lower = platform.to_lowercase();
    if p_lower.contains("windows") || p_lower.contains("linux") || p_lower.contains("android") {
        return ("legacy".to_owned(), 0);
    }
    ("unknown".to_owned(), 0)
}

pub fn get_or_assign_peer_ip(peer_id: &str) -> String {
    let clean = clean_peer_id(peer_id);
    if clean.is_empty() {
        return "10.144.0.1".to_owned();
    }
    let mut map = load_ip_map();
    if let Some(ip) = map.get(&clean) {
        if !ip.is_empty() {
            return ip.clone();
        }
    }
    let used: HashSet<String> = map.values().cloned().collect();
    let (mut x, mut y) = deterministic_lan_ip(&clean);
    let mut candidate = format!("10.144.{}.{}", x, y);
    while used.contains(&candidate) {
        y = y.wrapping_add(1);
        if y < 2 || y > 252 {
            y = 2;
            x = if x >= 252 { 2 } else { x + 1 };
        }
        candidate = format!("10.144.{}.{}", x, y);
    }
    map.insert(clean, candidate.clone());
    save_ip_map(&map);
    candidate
}

pub fn get_my_lan_ip() -> String {
    let my_id = Config::get_id();
    if my_id.is_empty() {
        "10.144.1.1".to_owned()
    } else {
        get_or_assign_peer_ip(&my_id)
    }
}

pub fn peer_has_saved_password(peer_id: &str) -> bool {
    let clean = clean_peer_id(peer_id);
    !PeerConfig::load(&clean).password.is_empty() || !PeerConfig::load(peer_id).password.is_empty()
}

pub fn ensure_init() {
    start_hot_update_background_loop();
    INIT_ONCE.call_once(|| {
        let enabled = load_enabled_peers();
        let enabled_bg = enabled.clone();
        std::thread::spawn(move || {
            cleanup_stale_macos_aliases(&enabled_bg);
        });
        for peer_id in enabled {
            if !peer_id.is_empty() {
                start_peer_lan(peer_id, None, None, false);
            }
        }
    });
}

pub fn get_peer_lan_state(peer_id: &str) -> PeerLanState {
    ensure_init();
    let clean = clean_peer_id(peer_id);
    let ip = get_or_assign_peer_ip(&clean);
    let has_password = peer_has_saved_password(peer_id);
    let (init_mode, init_build) = infer_initial_peer_mode(peer_id, &clean);
    let mut should_probe = false;
    let res = {
        let mut states = LAN_STATES.lock().unwrap();
        let entry = states.entry(clean.clone()).or_insert_with(|| PeerLanState {
            id: clean.clone(),
            ip: ip.clone(),
            enabled: false,
            status: "off".to_owned(),
            error: String::new(),
            has_password,
            remote_build: init_build,
            remote_updating: false,
            peer_mode: init_mode.clone(),
        });
        entry.ip = ip;
        entry.has_password = has_password;
        if entry.peer_mode.is_empty() || entry.peer_mode == "unknown" {
            entry.peer_mode = init_mode;
            if entry.remote_build == 0 && init_build > 0 {
                entry.remote_build = init_build;
            }
        }
        if has_password && !entry.enabled && !clean.is_empty() {
            let mut probed = PROBED_PEERS.lock().unwrap();
            if probed.insert(clean.clone()) {
                should_probe = true;
            }
        }
        entry.clone()
    };
    if should_probe {
        let peer_owned = peer_id.to_owned();
        let clean_owned = clean;
        std::thread::spawn(move || {
            if let Ok(rt) = tokio::runtime::Builder::new_Multi_thread_or_current() {
                let _ = rt.block_on(probe_peer_mode_once(&peer_owned, &clean_owned));
            }
        });
    }
    res
}

pub fn get_all_lan_states_json() -> String {
    ensure_init();
    let states = LAN_STATES.lock().unwrap();
    serde_json::to_string(&*states).unwrap_or_else(|_| "{}".to_owned())
}

#[derive(Deserialize)]
struct ToggleRequest {
    id: String,
    enable: bool,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    shared_password: Option<String>,
}

pub fn handle_toggle_json(json: &str) {
    ensure_init();
    let Ok(req) = serde_json::from_str::<ToggleRequest>(json) else {
        return;
    };
    if req.enable {
        let pass = req.password.filter(|s| !s.is_empty());
        let shared = req.shared_password.filter(|s| !s.is_empty());
        start_peer_lan(req.id, pass, shared, true);
    } else {
        stop_peer_lan(&req.id);
    }
}

pub fn start_peer_lan(
    peer_id: String,
    password: Option<String>,
    shared_password: Option<String>,
    interactive_alias: bool,
) {
    let clean = clean_peer_id(&peer_id);
    if clean.is_empty() {
        return;
    }
    let ip = get_or_assign_peer_ip(&clean);
    let (init_mode, init_build) = infer_initial_peer_mode(&peer_id, &clean);
    {
        let mut enabled = load_enabled_peers();
        enabled.insert(peer_id.clone());
        save_enabled_peers(&enabled);
    }
    {
        let mut states = LAN_STATES.lock().unwrap();
        let prev_mode = states
            .get(&clean)
            .map(|s| (s.peer_mode.clone(), s.remote_build))
            .unwrap_or((init_mode, init_build));
        states.insert(
            clean.clone(),
            PeerLanState {
                id: clean.clone(),
                ip: ip.clone(),
                enabled: true,
                status: "connecting".to_owned(),
                error: String::new(),
                has_password: peer_has_saved_password(&peer_id) || password.is_some(),
                remote_build: prev_mode.1,
                remote_updating: false,
                peer_mode: prev_mode.0,
            },
        );
    }
    {
        let mut workers = ACTIVE_WORKERS.lock().unwrap();
        if let Some(old) = workers.remove(&clean) {
            let _ = old.stop_tx.send(true);
        }
        let (stop_tx, stop_rx) = watch::channel(false);
        workers.insert(clean.clone(), ActivePeerWorker { stop_tx });
        std::thread::spawn(move || {
            let Ok(rt) = tokio::runtime::Builder::new_Multi_thread_or_current() else {
                return;
            };
            rt.block_on(run_peer_lan_loop(
                peer_id,
                clean,
                ip,
                password,
                shared_password,
                interactive_alias,
                stop_rx,
            ));
        });
    }
}

trait RuntimeBuilderExt {
    fn new_Multi_thread_or_current() -> std::io::Result<tokio::runtime::Runtime>;
}

impl RuntimeBuilderExt for tokio::runtime::Builder {
    fn new_Multi_thread_or_current() -> std::io::Result<tokio::runtime::Runtime> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
    }
}

pub fn stop_peer_lan(peer_id: &str) {
    let clean = clean_peer_id(peer_id);
    {
        let mut enabled = load_enabled_peers();
        enabled.retain(|id| clean_peer_id(id) != clean);
        save_enabled_peers(&enabled);
    }
    ACTIVE_TUNNELS.lock().unwrap().remove(&clean);
    if let Some(w) = ACTIVE_WORKERS.lock().unwrap().remove(&clean) {
        let _ = w.stop_tx.send(true);
    }
    let ip = get_or_assign_peer_ip(&clean);
    remove_pf_rules_for_ip(&ip);
    unmount_local_ip_alias(&ip);
    let (init_mode, init_build) = infer_initial_peer_mode(peer_id, &clean);
    let mut states = LAN_STATES.lock().unwrap();
    let prev = states
        .get(&clean)
        .map(|s| (s.peer_mode.clone(), s.remote_build))
        .unwrap_or((init_mode, init_build));
    states.insert(
        clean.clone(),
        PeerLanState {
            id: clean,
            ip,
            enabled: false,
            status: "off".to_owned(),
            error: String::new(),
            has_password: peer_has_saved_password(peer_id),
            remote_build: prev.1,
            remote_updating: false,
            peer_mode: prev.0,
        },
    );
}

fn update_state(clean_id: &str, status: &str, error: &str) {
    let mut states = LAN_STATES.lock().unwrap();
    if let Some(st) = states.get_mut(clean_id) {
        if !st.enabled && status != "off" {
            return;
        }
        st.status = status.to_owned();
        st.error = error.to_owned();
        st.has_password = peer_has_saved_password(clean_id);
    }
}

fn update_peer_remote_meta(clean_id: &str, meta: &str) {
    if meta.is_empty() {
        return;
    }
    let mut r_build = 0u64;
    let mut r_updating = false;
    for part in meta.split(';') {
        if let Some(b) = part.strip_prefix("build=") {
            if let Ok(v) = b.trim().parse::<u64>() {
                r_build = v;
            }
        } else if let Some(u) = part.strip_prefix("updating=") {
            r_updating = u.trim() == "1";
        }
    }
    if r_build > 0 {
        save_peer_mode(clean_id, "custom", r_build);
    }
    let mut states = LAN_STATES.lock().unwrap();
    if let Some(st) = states.get_mut(clean_id) {
        if r_build > 0 {
            st.remote_build = r_build;
            st.peer_mode = "custom".to_owned();
        }
        st.remote_updating = r_updating;
    }
}

fn has_macos_sudo_pfctl() -> bool {
    #[cfg(target_os = "macos")]
    {
        std::process::Command::new("sudo")
            .args(["-n", "/sbin/pfctl", "-s", "info"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }
    #[cfg(not(target_os = "macos"))]
    {
        true
    }
}

#[cfg(target_os = "linux")]
fn linux_bin_path(name: &str) -> String {
    ["/usr/sbin", "/usr/bin", "/sbin", "/bin"]
        .iter()
        .map(|dir| format!("{}/{}", dir, name))
        .find(|path| std::path::Path::new(path).exists())
        .unwrap_or_else(|| name.to_owned())
}

#[cfg(target_os = "linux")]
fn linux_alias_present(ip: &str) -> bool {
    std::process::Command::new(linux_bin_path("ip"))
        .args(["addr", "show", "dev", "lo"])
        .output()
        .map(|out| String::from_utf8_lossy(&out.stdout).contains(&format!("inet {}/32", ip)))
        .unwrap_or(false)
}

#[cfg(target_os = "linux")]
fn linux_is_root() -> bool {
    let euid = unsafe { hbb_common::libc::geteuid() };
    euid == 0
}

#[cfg(target_os = "linux")]
fn linux_run(bin: &str, args: &[&str]) -> bool {
    let direct = std::process::Command::new(bin)
        .args(args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if direct {
        return true;
    }
    std::process::Command::new("sudo")
        .arg("-n")
        .arg(bin)
        .args(args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[cfg(target_os = "linux")]
fn has_linux_sudo() -> bool {
    let ip_bin = linux_bin_path("ip");
    std::process::Command::new("sudo")
        .arg("-n")
        .arg(ip_bin)
        .args(["addr", "show", "dev", "lo"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[cfg(target_os = "linux")]
fn install_linux_sudoers_and_alias(ip: Option<&str>) -> Result<(), String> {
    let user = std::env::var("USER").unwrap_or_default();
    let user = if !user.is_empty()
        && user
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        user
    } else {
        "ALL".to_owned()
    };
    let ip_bin = linux_bin_path("ip");
    let iptables_bin = linux_bin_path("iptables");
    let sudoers = format!(
        "{user} ALL=(root) NOPASSWD: {ip_bin} addr add 10.144.*, {ip_bin} addr del 10.144.*, {ip_bin} addr show dev lo, {iptables_bin} -t nat *\n"
    );
    let alias_cmd = ip
        .filter(|s| s.starts_with("10.144."))
        .map(|s| format!(" && {ip_bin} addr add {s}/32 dev lo"))
        .unwrap_or_default();
    let cmd = format!(
        "printf \"%s\" \"{sudoers}\" > /etc/sudoers.d/rustdesk-lan && chmod 0440 /etc/sudoers.d/rustdesk-lan && visudo -c -f /etc/sudoers.d/rustdesk-lan >/dev/null{alias_cmd}"
    );
    if crate::platform::run_cmds_privileged(&cmd) {
        Ok(())
    } else {
        Err("未授予局域网 IP 与端口转发权限".to_owned())
    }
}

#[cfg(target_os = "linux")]
fn sync_linux_nat_rules() {
    if !linux_is_root() && !has_linux_sudo() {
        return;
    }
    let entries: Vec<((String, u16), u16)> = {
        let map = LINUX_NAT_PORTS.lock().unwrap();
        let mut v: Vec<_> = map.iter().map(|(k, relay)| (k.clone(), *relay)).collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    };
    let iptables_bin = linux_bin_path("iptables");
    let run = |args: &[&str]| -> bool { linux_run(&iptables_bin, args) };
    run(&["-t", "nat", "-N", "RUSTDESK_LAN"]);
    if !run(&["-t", "nat", "-F", "RUSTDESK_LAN"]) {
        return;
    }
    if !run(&["-t", "nat", "-C", "OUTPUT", "-j", "RUSTDESK_LAN"]) {
        run(&["-t", "nat", "-A", "OUTPUT", "-j", "RUSTDESK_LAN"]);
    }
    for ((ip, port), relay_port) in &entries {
        run(&[
            "-t",
            "nat",
            "-A",
            "RUSTDESK_LAN",
            "-d",
            ip,
            "-p",
            "tcp",
            "--dport",
            &port.to_string(),
            "-j",
            "REDIRECT",
            "--to-ports",
            &relay_port.to_string(),
        ]);
    }
}

#[cfg(target_os = "linux")]
fn remove_linux_nat_ports_for_ip(ip: &str) {
    let changed = {
        let mut map = LINUX_NAT_PORTS.lock().unwrap();
        let before = map.len();
        map.retain(|(key_ip, _), _| key_ip != ip);
        map.len() != before
    };
    if changed {
        sync_linux_nat_rules();
    }
}

fn install_macos_sudoers_and_alias(ip: Option<&str>) -> Result<(), String> {
    #[cfg(target_os = "macos")]
    {
        let alias_cmd = ip
            .filter(|s| s.starts_with("10.144."))
            .map(|s| format!("/sbin/ifconfig lo0 alias {} up && ", s))
            .unwrap_or_default();
        let script = format!(
            "do shell script \"{alias_cmd}mkdir -p /etc/sudoers.d && printf 'ALL ALL=(ALL) NOPASSWD: /sbin/ifconfig, /sbin/pfctl, /usr/bin/ditto, /usr/bin/xattr, /usr/bin/codesign, /usr/bin/pkill, /bin/mv, /bin/rm\\\\n' > /etc/sudoers.d/rustdesk_lan && chmod 0440 /etc/sudoers.d/rustdesk_lan\" with administrator privileges"
        );
        match std::process::Command::new("/usr/bin/osascript")
            .args(["-e", &script])
            .status()
        {
            Ok(s) if s.success() => Ok(()),
            _ => Err("未授予网络接口与端口转发权限".to_owned()),
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = ip;
        Ok(())
    }
}

fn cleanup_stale_macos_aliases(enabled_peers: &HashSet<String>) {
    #[cfg(target_os = "macos")]
    {
        let my_ip = get_my_lan_ip();
        let mut enabled_ips: HashSet<String> = enabled_peers
            .iter()
            .map(|id| get_or_assign_peer_ip(&clean_peer_id(id)))
            .collect();
        enabled_ips.insert(my_ip);

        let mut stale_ips = Vec::new();
        let mut any_lan_alias = false;
        if let Ok(out) = std::process::Command::new("/sbin/ifconfig")
            .arg("lo0")
            .output()
        {
            let text = String::from_utf8_lossy(&out.stdout);
            for line in text.lines() {
                let line = line.trim();
                if let Some(rest) = line.strip_prefix("inet ") {
                    if let Some(ip) = rest.split_whitespace().next() {
                        if ip.starts_with("10.144.") {
                            any_lan_alias = true;
                            if !enabled_ips.contains(ip) {
                                stale_ips.push(ip.to_owned());
                            }
                        }
                    }
                }
            }
        }
        if (any_lan_alias || !enabled_peers.is_empty())
            && !has_macos_sudo_pfctl()
            && !PF_SUDO_CHECKED.swap(true, Ordering::SeqCst)
        {
            let _ = install_macos_sudoers_and_alias(None);
        }
        for ip in stale_ips {
            unmount_local_ip_alias(&ip);
        }
        sync_macos_pf_rules();
    }
    #[cfg(target_os = "linux")]
    {
        let my_ip = get_my_lan_ip();
        let mut enabled_ips: HashSet<String> = enabled_peers
            .iter()
            .map(|id| get_or_assign_peer_ip(&clean_peer_id(id)))
            .collect();
        enabled_ips.insert(my_ip);
        let mut stale_ips = Vec::new();
        if let Ok(out) = std::process::Command::new(linux_bin_path("ip"))
            .args(["addr", "show", "dev", "lo"])
            .output()
        {
            for line in String::from_utf8_lossy(&out.stdout).lines() {
                let line = line.trim();
                if let Some(rest) = line.strip_prefix("inet ") {
                    if let Some(ip) = rest.split('/').next() {
                        if ip.starts_with("10.144.") && !enabled_ips.contains(ip) {
                            stale_ips.push(ip.to_owned());
                        }
                    }
                }
            }
        }
        for ip in stale_ips {
            unmount_local_ip_alias(&ip);
        }
        sync_macos_pf_rules();
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = enabled_peers;
    }
}

pub fn unmount_local_ip_alias(ip: &str) {
    if !ip.starts_with("10.144.") || ip == get_my_lan_ip() {
        return;
    }
    ALIAS_MOUNTED.lock().unwrap().remove(ip);
    #[cfg(target_os = "macos")]
    {
        let _ = std::process::Command::new("sudo")
            .args(["-n", "/sbin/ifconfig", "lo0", "-alias", ip])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
    #[cfg(target_os = "linux")]
    {
        let ip_bin = linux_bin_path("ip");
        let alias = format!("{}/32", ip);
        let _ = linux_run(&ip_bin, &["addr", "del", alias.as_str(), "dev", "lo"]);
    }
}

pub fn remove_pf_rules_for_ip(ip: &str) {
    #[cfg(target_os = "macos")]
    {
        let mut map = MACOS_PF_PORTS.lock().unwrap();
        let before = map.len();
        map.retain(|(k_ip, _), _| k_ip != ip);
        let changed = map.len() != before;
        drop(map);
        if changed {
            sync_macos_pf_rules();
        }
    }
    #[cfg(target_os = "linux")]
    {
        remove_linux_nat_ports_for_ip(ip);
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = ip;
    }
}

pub fn sync_macos_pf_rules() {
    #[cfg(target_os = "linux")]
    {
        sync_linux_nat_rules();
    }
    #[cfg(target_os = "macos")]
    {
        if !has_macos_sudo_pfctl() {
            return;
        }
        let entries: Vec<((String, u16), Option<u16>)> = {
            let map = MACOS_PF_PORTS.lock().unwrap();
            let mut v: Vec<_> = map.iter().map(|(k, val)| (k.clone(), *val)).collect();
            v.sort_by(|a, b| a.0.cmp(&b.0));
            v
        };
        let my_ip = get_my_lan_ip();
        let mut rdr_lines = String::new();
        let mut filter_lines = String::new();
        for ((ip, port), relay_opt) in &entries {
            if let Some(relay_port) = relay_opt {
                rdr_lines.push_str(&format!(
                    "rdr pass on lo0 inet proto tcp from any to {} port = {} -> {} port {}\n",
                    ip, port, ip, relay_port
                ));
                filter_lines.push_str(&format!(
                    "pass in quick on lo0 inet proto tcp from any to {} port = {} no state\n",
                    ip, relay_port
                ));
            } else {
                filter_lines.push_str(&format!(
                    "pass in quick on lo0 inet proto tcp from any to {} port = {} no state\n",
                    ip, port
                ));
            }
        }
        if my_ip.starts_with("10.144.") {
            filter_lines.push_str(&format!(
                "pass in quick on lo0 inet proto tcp from any to {} no state\n",
                my_ip
            ));
        }
        filter_lines.push_str(
            "block return-rst in quick on lo0 inet proto tcp from any to 10.144.0.0/16 port 1:49151 flags S/SA\n",
        );

        let full_conf = format!("{}{}", rdr_lines, filter_lines);
        let _ = std::fs::write("/tmp/rustdesk_pf.conf", &full_conf);
        let _ = std::fs::write("/tmp/rustdesk_pf_filter.conf", &filter_lines);
        let _ = std::process::Command::new("sudo")
            .args([
                "-n",
                "/sbin/pfctl",
                "-a",
                "com.apple/rustdesk_lan",
                "-f",
                "/tmp/rustdesk_pf.conf",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        let _ = std::process::Command::new("sudo")
            .args([
                "-n",
                "/sbin/pfctl",
                "-a",
                "com.apple/250.ApplicationFirewall/rustdesk_lan",
                "-f",
                "/tmp/rustdesk_pf_filter.conf",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        let _ = std::process::Command::new("sudo")
            .args(["-n", "/sbin/pfctl", "-E"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
}

async fn bind_lan_port(ip: &str, port: u16) -> std::io::Result<TcpListener> {
    #[cfg(target_os = "macos")]
    {
        if port >= 1024 {
            if let Ok(listener) = TcpListener::bind(format!("{}:{}", ip, port)).await {
                MACOS_PF_PORTS
                    .lock()
                    .unwrap()
                    .insert((ip.to_owned(), port), None);
                return Ok(listener);
            }
        }
        let listener = TcpListener::bind(format!("{}:0", ip)).await?;
        let relay_port = listener.local_addr()?.port();
        MACOS_PF_PORTS
            .lock()
            .unwrap()
            .insert((ip.to_owned(), port), Some(relay_port));
        Ok(listener)
    }
    #[cfg(target_os = "linux")]
    {
        if let Ok(listener) = TcpListener::bind(format!("{}:{}", ip, port)).await {
            return Ok(listener);
        }
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let relay_port = listener.local_addr()?.port();
        LINUX_NAT_PORTS
            .lock()
            .unwrap()
            .insert((ip.to_owned(), port), relay_port);
        Ok(listener)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        TcpListener::bind(format!("{}:{}", ip, port)).await
    }
}

pub fn ensure_local_ip_alias(ip: &str, allow_prompt: bool) -> Result<(), String> {
    if !ip.starts_with("10.144.") {
        return Err("Invalid LAN IP".to_owned());
    }
    #[cfg(target_os = "macos")]
    {
        let sudo_ok = has_macos_sudo_pfctl();
        if ALIAS_MOUNTED.lock().unwrap().contains(ip) && sudo_ok {
            return Ok(());
        }
        let mut already_on_lo0 = false;
        if let Ok(out) = std::process::Command::new("/sbin/ifconfig")
            .arg("lo0")
            .output()
        {
            let text = String::from_utf8_lossy(&out.stdout);
            if text.contains(&format!("inet {} ", ip)) {
                already_on_lo0 = true;
            }
        }
        if sudo_ok {
            if !already_on_lo0 {
                let status = std::process::Command::new("sudo")
                    .args(["-n", "/sbin/ifconfig", "lo0", "alias", ip, "up"])
                    .status()
                    .map_err(|e| e.to_string())?;
                if !status.success() {
                    return Err("挂载局域网 IP 失败".to_owned());
                }
            }
            ALIAS_MOUNTED.lock().unwrap().insert(ip.to_owned());
            sync_macos_pf_rules();
            return Ok(());
        }
        if !allow_prompt && PF_SUDO_CHECKED.swap(true, Ordering::SeqCst) {
            return Err("需要管理员权限配置局域网端口转发".to_owned());
        }
        PF_SUDO_CHECKED.store(true, Ordering::SeqCst);
        install_macos_sudoers_and_alias(Some(ip))?;
        ALIAS_MOUNTED.lock().unwrap().insert(ip.to_owned());
        sync_macos_pf_rules();
        Ok(())
    }
    #[cfg(target_os = "windows")]
    {
        if ALIAS_MOUNTED.lock().unwrap().contains(ip) {
            return Ok(());
        }
        use std::os::windows::process::CommandExt;
        let _ = allow_prompt;
        let _ = std::process::Command::new("netsh")
            .args([
                "interface",
                "ipv4",
                "add",
                "address",
                "Loopback Pseudo-Interface 1",
                ip,
                "255.255.255.255",
            ])
            .creation_flags(0x08000000)
            .status();
        ALIAS_MOUNTED.lock().unwrap().insert(ip.to_owned());
        Ok(())
    }
    #[cfg(target_os = "linux")]
    {
        let ip_bin = linux_bin_path("ip");
        let alias = format!("{}/32", ip);
        if ALIAS_MOUNTED.lock().unwrap().contains(ip) && linux_alias_present(ip) {
            return Ok(());
        }
        if !linux_alias_present(ip) {
            let _ = linux_run(&ip_bin, &["addr", "add", alias.as_str(), "dev", "lo"]);
        }
        if !linux_alias_present(ip) {
            if !allow_prompt && PF_SUDO_CHECKED.swap(true, Ordering::SeqCst) {
                return Err("需要管理员权限配置局域网 IP".to_owned());
            }
            PF_SUDO_CHECKED.store(true, Ordering::SeqCst);
            install_linux_sudoers_and_alias(Some(ip))?;
            if !linux_alias_present(ip) {
                return Err("挂载局域网 IP 失败".to_owned());
            }
        }
        ALIAS_MOUNTED.lock().unwrap().insert(ip.to_owned());
        sync_macos_pf_rules();
        Ok(())
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows", target_os = "linux")))]
    {
        let _ = allow_prompt;
        Ok(())
    }
}

pub fn get_local_listening_ports() -> Vec<u16> {
    let mut ports: HashSet<u16> = DEFAULT_LAN_PORTS.iter().copied().collect();
    #[cfg(target_os = "macos")]
    {
        if let Ok(out) = std::process::Command::new("/usr/sbin/netstat")
            .args(["-an", "-p", "tcp"])
            .output()
        {
            let text = String::from_utf8_lossy(&out.stdout);
            for line in text.lines() {
                if !line.contains("LISTEN") {
                    continue;
                }
                let cols: Vec<&str> = line.split_whitespace().collect();
                if cols.len() >= 4 {
                    let local = cols[3];
                    if local.starts_with("10.144.") {
                        continue;
                    }
                    if let Some(port_str) = local.rsplit('.').next() {
                        if let Ok(p) = port_str.parse::<u16>() {
                            if p > 0 && p < 49152 {
                                ports.insert(p);
                            }
                        }
                    }
                }
            }
        }
    }
    #[cfg(target_os = "linux")]
    {
        for path in ["/proc/net/tcp", "/proc/net/tcp6"] {
            if let Ok(content) = std::fs::read_to_string(path) {
                for line in content.lines().skip(1) {
                    let cols: Vec<&str> = line.split_whitespace().collect();
                    if cols.len() >= 4 && cols[3] == "0A" {
                        if let Some((addr_hex, port_hex)) = cols[1].split_once(':') {
                            if addr_hex.ends_with("900A") {
                                continue;
                            }
                            if let Ok(p) = u16::from_str_radix(port_hex, 16) {
                                if p > 0 && p < 49152 {
                                    ports.insert(p);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        if let Ok(out) = std::process::Command::new("netstat")
            .args(["-ano", "-p", "tcp"])
            .creation_flags(0x08000000)
            .output()
        {
            let text = String::from_utf8_lossy(&out.stdout);
            for line in text.lines() {
                if !line.contains("LISTENING") {
                    continue;
                }
                let cols: Vec<&str> = line.split_whitespace().collect();
                if cols.len() >= 2 {
                    let local = cols[1];
                    if local.starts_with("10.144.") {
                        continue;
                    }
                    if let Some(port_str) = local.rsplit(':').next() {
                        if let Ok(p) = port_str.parse::<u16>() {
                            if p > 0 {
                                ports.insert(p);
                            }
                        }
                    }
                }
            }
        }
    }
    let mut sorted: Vec<u16> = ports.into_iter().collect();
    sorted.sort_unstable();
    sorted
}

pub fn get_local_listening_ports_csv() -> String {
    get_local_listening_ports()
        .into_iter()
        .map(|p| p.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

#[derive(Clone)]
struct LanClientInterface {
    lc: Arc<RwLock<LoginConfigHandler>>,
    clean_id: String,
    need_password: Arc<Mutex<bool>>,
    last_error: Arc<Mutex<String>>,
}

#[async_trait]
impl Interface for LanClientInterface {
    fn send(&self, _data: Data) {}

    fn set_multiple_windows_session(&self, _sessions: Vec<WindowsSession>) {}

    fn msgbox(&self, msgtype: &str, _title: &str, text: &str, _link: &str) {
        if msgtype.contains("input-password") || msgtype.contains("re-input-password") {
            *self.need_password.lock().unwrap() = true;
        } else if msgtype.contains("error") && !text.is_empty() {
            *self.last_error.lock().unwrap() = text.to_owned();
        }
    }

    fn handle_login_error(&self, err: &str) -> bool {
        if err.contains("pasword") || err.contains("Password") || err.contains("password") {
            *self.need_password.lock().unwrap() = true;
        }
        *self.last_error.lock().unwrap() = err.to_owned();
        false
    }

    fn handle_peer_info(&self, pi: PeerInfo) {
        self.lc.write().unwrap().handle_peer_info(&pi);
    }

    async fn handle_hash(
        &self,
        pass: &str,
        hash: Hash,
        trust: &mut crate::trusted_devices::client::Session,
        peer: &mut Stream,
    ) -> bool {
        crate::client::handle_hash(self.lc.clone(), pass, hash, self, trust, peer).await
    }

    async fn handle_login_from_ui(
        &self,
        os_username: String,
        os_password: String,
        password: String,
        remember: bool,
        trust: &mut crate::trusted_devices::client::Session,
        peer: &mut Stream,
    ) {
        allow_err!(
            crate::client::handle_login_from_ui(
                self.lc.clone(),
                os_username,
                os_password,
                password,
                remember,
                trust,
                peer,
            )
            .await
        );
    }

    async fn handle_test_delay(&self, t: TestDelay, peer: &mut Stream) {
        crate::client::handle_test_delay(t, peer).await;
    }

    fn get_lch(&self) -> Arc<RwLock<LoginConfigHandler>> {
        self.lc.clone()
    }
}

impl LanClientInterface {
    fn save_verified_password(&self) {
        let mut lc = self.lc.write().unwrap();
        if !lc.password.is_empty() {
            let mut cfg = lc.load_config();
            if cfg.password != lc.password {
                cfg.password = lc.password.clone();
                lc.save_config(cfg);
            }
        }
    }
}

enum LanTunnelConn {
    Stream(Stream, bool),
    LegacyPeer,
}

async fn probe_peer_mode_once(peer_id: &str, clean_id: &str) -> ResultType<()> {
    let token = LocalConfig::get_option("access_token");
    let key = crate::get_key(false).await;
    let mut lc = LoginConfigHandler::default();
    lc.initialize(
        peer_id.to_owned(),
        ConnType::PORT_FORWARD,
        None,
        false,
        None,
        None,
        None,
    );
    lc.port_forward = ("LAN".to_owned(), 0);
    lc.port_forward_multiplex = true;
    lc.port_forward_mux = true;
    let iface = LanClientInterface {
        lc: Arc::new(RwLock::new(lc)),
        clean_id: clean_id.to_owned(),
        need_password: Arc::new(Mutex::new(false)),
        last_error: Arc::new(Mutex::new(String::new())),
    };
    let (_stop_tx, mut stop_rx) = watch::channel(false);
    match connect_lan_tunnel(peer_id, "", &key, &token, iface.clone(), &mut stop_rx).await {
        Ok(Some(LanTunnelConn::LegacyPeer)) => {
            save_peer_mode(clean_id, "legacy", 0);
        }
        Ok(Some(LanTunnelConn::Stream(stream, mux))) => {
            if !mux {
                save_peer_mode(clean_id, "legacy", 0);
            } else {
                let tunnel = Tunnel::new();
                let handle = tunnel.set_muxed(stream, iface);
                let _ = handle.probe_remote_ports("__lan_ports_probe__").await;
                sleep(Duration::from_millis(350)).await;
                let meta = handle.remote_meta();
                if !meta.is_empty() || !handle.remote_ports().is_empty() {
                    update_peer_remote_meta(clean_id, &meta);
                    let build = LAN_STATES
                        .lock()
                        .unwrap()
                        .get(clean_id)
                        .map(|s| s.remote_build)
                        .unwrap_or(0);
                    save_peer_mode(clean_id, "custom", build.max(1));
                } else {
                    save_peer_mode(clean_id, "legacy", 0);
                }
            }
        }
        _ => {}
    }
    Ok(())
}

async fn run_peer_lan_loop(
    peer_id: String,
    clean_id: String,
    ip: String,
    explicit_password: Option<String>,
    shared_password: Option<String>,
    interactive_alias: bool,
    mut stop_rx: watch::Receiver<bool>,
) {
    let _ = ensure_local_ip_alias(&get_my_lan_ip(), interactive_alias);

    let token = LocalConfig::get_option("access_token");
    let key = crate::get_key(false).await;

    loop {
        if *stop_rx.borrow() {
            break;
        }
        update_state(&clean_id, "connecting", "");

        let mut lc = LoginConfigHandler::default();
        lc.initialize(
            peer_id.clone(),
            ConnType::PORT_FORWARD,
            None,
            false,
            None,
            shared_password.clone(),
            None,
        );
        lc.port_forward = ("LAN".to_owned(), 0);
        lc.port_forward_multiplex = true;
        lc.port_forward_mux = true;
        let lc = Arc::new(RwLock::new(lc));

        let need_password = Arc::new(Mutex::new(false));
        let last_error = Arc::new(Mutex::new(String::new()));
        let iface = LanClientInterface {
            lc: lc.clone(),
            clean_id: clean_id.clone(),
            need_password: need_password.clone(),
            last_error: last_error.clone(),
        };

        let pass_to_use = explicit_password.clone().unwrap_or_default();
        match connect_lan_tunnel(&peer_id, &pass_to_use, &key, &token, iface.clone(), &mut stop_rx)
            .await
        {
            Ok(Some(LanTunnelConn::Stream(stream, mux))) => {
                if let Err(err) = ensure_local_ip_alias(&ip, interactive_alias) {
                    update_state(&clean_id, "error", &err);
                    break;
                }
                if !mux {
                    save_peer_mode(&clean_id, "legacy", 0);
                }
                update_state(&clean_id, "connected", "");
                let tunnel = Tunnel::new();
                let handle = if mux {
                    Some(tunnel.set_muxed(stream, iface.clone()))
                } else {
                    None
                };
                run_bound_listeners_for_peer(
                    &peer_id,
                    &ip,
                    &pass_to_use,
                    shared_password.clone(),
                    &key,
                    &token,
                    handle,
                    &mut stop_rx,
                )
                .await;
                unmount_local_ip_alias(&ip);
            }
            Ok(Some(LanTunnelConn::LegacyPeer)) => {
                save_peer_mode(&clean_id, "legacy", 0);
                if let Err(err) = ensure_local_ip_alias(&ip, interactive_alias) {
                    update_state(&clean_id, "error", &err);
                    break;
                }
                update_state(&clean_id, "connected", "");
                run_bound_listeners_for_peer(
                    &peer_id,
                    &ip,
                    &pass_to_use,
                    shared_password.clone(),
                    &key,
                    &token,
                    None,
                    &mut stop_rx,
                )
                .await;
                unmount_local_ip_alias(&ip);
            }
            Ok(None) => {
                remove_pf_rules_for_ip(&ip);
                unmount_local_ip_alias(&ip);
                if *stop_rx.borrow() {
                    break;
                }
                if *need_password.lock().unwrap() {
                    update_state(&clean_id, "need_password", "需要输入连接密码");
                    break;
                }
                let err = last_error.lock().unwrap().clone();
                update_state(
                    &clean_id,
                    "error",
                    if err.is_empty() { "连接失败" } else { &err },
                );
            }
            Err(e) => {
                remove_pf_rules_for_ip(&ip);
                unmount_local_ip_alias(&ip);
                if *stop_rx.borrow() {
                    break;
                }
                if *need_password.lock().unwrap() {
                    update_state(&clean_id, "need_password", "需要输入连接密码");
                    break;
                }
                update_state(&clean_id, "error", &e.to_string());
            }
        }

        tokio::select! {
            _ = sleep(Duration::from_secs(3)) => {}
            _ = stop_rx.changed() => break,
        }
    }
}

async fn connect_lan_tunnel(
    peer_id: &str,
    explicit_password: &str,
    key: &str,
    token: &str,
    interface: LanClientInterface,
    stop_rx: &mut watch::Receiver<bool>,
) -> ResultType<Option<LanTunnelConn>> {
    let ((mut stream, direct, pk, _kcp, _stream_type), (feedback, rendezvous_server)) =
        Client::start(peer_id, key, token, ConnType::PORT_FORWARD, interface.clone()).await?;
    let mut trust = crate::trusted_devices::client::Session::new(
        &interface.get_lch().read().unwrap(),
        stream.is_secured(),
        pk.as_deref(),
    )?;
    interface.update_direct(Some(direct));
    let _keep_it = hc_connection(feedback, rendezvous_server, token).await;
    let mux;
    loop {
        tokio::select! {
            _ = stop_rx.changed() => return Ok(None),
            res = timeout(READ_TIMEOUT, stream.next()) => match res {
                Err(_) => hbb_common::bail!("连接超时"),
                Ok(Some(Ok(bytes))) => {
                    let msg_in = Message::parse_from_bytes(&bytes)?;
                    match msg_in.union {
                        Some(message::Union::Hash(hash)) => {
                            {
                                let lc = interface.get_lch();
                                let mut w = lc.write().unwrap();
                                w.port_forward = ("LAN".to_owned(), 0);
                                w.port_forward_multiplex = true;
                                w.set_hash(hash.clone());
                            }
                            if !explicit_password.is_empty() {
                                interface.handle_login_from_ui(
                                    String::new(),
                                    String::new(),
                                    explicit_password.to_owned(),
                                    true,
                                    &mut trust,
                                    &mut stream,
                                ).await;
                            } else if !interface.handle_hash("", hash, &mut trust, &mut stream).await {
                                *interface.need_password.lock().unwrap() = true;
                                return Ok(None);
                            }
                        }
                        Some(message::Union::LoginResponse(lr)) => match trust.respond(lr, &interface).await.union {
                            Some(login_response::Union::Error(err)) => {
                                if err.contains("LAN:0") {
                                    interface.save_verified_password();
                                    return Ok(Some(LanTunnelConn::LegacyPeer));
                                }
                                interface.handle_login_error(&err);
                                return Ok(None);
                            }
                            Some(login_response::Union::PeerInfo(pi)) => {
                                mux = pi.features.as_ref().map(|f| f.port_forward_mux).unwrap_or(false);
                                interface.handle_peer_info(pi);
                                break;
                            }
                            _ => {}
                        },
                        Some(message::Union::TestDelay(t)) => {
                            interface.handle_test_delay(t, &mut stream).await;
                        }
                        _ => {}
                    }
                }
                Ok(Some(Err(e))) => hbb_common::bail!("连接断开: {}", e),
                _ => hbb_common::bail!("对端重置了连接"),
            }
        }
    }
    Ok(Some(LanTunnelConn::Stream(stream, mux)))
}

async fn run_bound_listeners_for_peer(
    peer_id: &str,
    ip: &str,
    password: &str,
    shared_password: Option<String>,
    key: &str,
    token: &str,
    handle: Option<Arc<TunnelHandle>>,
    stop_rx: &mut watch::Receiver<bool>,
) {
    let mut bound_ports: HashMap<u16, watch::Sender<bool>> = HashMap::new();
    let my_ip = get_my_lan_ip();
    let clean = clean_peer_id(peer_id);
    if let Some(h) = &handle {
        ACTIVE_TUNNELS.lock().unwrap().insert(clean.clone(), h.clone());
    }
    let mut first_probe = true;

    loop {
        if *stop_rx.borrow() {
            break;
        }
        if let Some(h) = &handle {
            if h.is_closed() {
                break;
            }
            let target_build = TARGET_UPDATE_BUILD
                .load(Ordering::Relaxed)
                .max(get_local_build_number());
            let force_seq = FORCE_UPDATE_SEQ.load(Ordering::Relaxed);
            let probe = format!(
                "__lan_ports__:{}:{},;build={};force={}",
                my_ip,
                get_local_listening_ports_csv(),
                target_build,
                force_seq
            );
            let _ = h.probe_remote_ports(&probe).await;
            if first_probe && h.remote_ports().is_empty() {
                first_probe = false;
                sleep(Duration::from_millis(250)).await;
                if h.remote_ports().is_empty() && h.remote_meta().is_empty() {
                    save_peer_mode(&clean, "legacy", 0);
                }
            }
            update_peer_remote_meta(&clean, &h.remote_meta());
        }

        let mut desired: HashSet<u16> = DEFAULT_LAN_PORTS.iter().copied().collect();
        if let Some(h) = &handle {
            for p in h.remote_ports() {
                desired.insert(p);
            }
        }

        let mut pf_dirty = false;
        for port in desired {
            if bound_ports.contains_key(&port) {
                continue;
            }
            if let Ok(listener) = bind_lan_port(ip, port).await {
                pf_dirty = true;
                let (port_stop_tx, mut port_stop_rx) = watch::channel(false);
                bound_ports.insert(port, port_stop_tx);
                let handle_clone = handle.clone();
                let peer_id = peer_id.to_owned();
                let password = password.to_owned();
                let shared_password = shared_password.clone();
                let key = key.to_owned();
                let token = token.to_owned();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = port_stop_rx.changed() => break,
                            res = listener.accept() => match res {
                                Ok((socket, _)) => {
                                    let use_mux = handle_clone
                                        .as_ref()
                                        .map(|h| !h.is_closed() && !h.remote_ports().is_empty())
                                        .unwrap_or(false);
                                    if use_mux {
                                        if let Some(h) = &handle_clone {
                                            let _ = h.open("127.0.0.1", port as i32, socket, Vec::new());
                                        }
                                    } else {
                                        let peer_id = peer_id.clone();
                                        let password = password.clone();
                                        let shared_password = shared_password.clone();
                                        let key = key.clone();
                                        let token = token.clone();
                                        tokio::spawn(async move {
                                            let _ = forward_legacy_port(
                                                &peer_id,
                                                &password,
                                                shared_password,
                                                &key,
                                                &token,
                                                socket,
                                                port as i32,
                                            )
                                            .await;
                                        });
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                    }
                });
            }
        }
        if pf_dirty {
            sync_macos_pf_rules();
        }

        tokio::select! {
            _ = sleep(Duration::from_secs(3)) => {}
            _ = stop_rx.changed() => break,
        }
    }

    ACTIVE_TUNNELS.lock().unwrap().remove(&clean);
    for (_, tx) in bound_ports {
        let _ = tx.send(true);
    }
    remove_pf_rules_for_ip(ip);
}

async fn forward_legacy_port(
    peer_id: &str,
    password: &str,
    shared_password: Option<String>,
    key: &str,
    token: &str,
    socket: TcpStream,
    remote_port: i32,
) -> ResultType<()> {
    let mut lc = LoginConfigHandler::default();
    lc.initialize(
        peer_id.to_owned(),
        ConnType::PORT_FORWARD,
        None,
        false,
        None,
        shared_password,
        None,
    );
    lc.port_forward = ("127.0.0.1".to_owned(), remote_port);
    lc.port_forward_multiplex = false;
    lc.port_forward_mux = false;
    let lc = Arc::new(RwLock::new(lc));
    let interface = LanClientInterface {
        lc: lc.clone(),
        clean_id: clean_peer_id(peer_id),
        need_password: Arc::new(Mutex::new(false)),
        last_error: Arc::new(Mutex::new(String::new())),
    };

    let ((mut stream, direct, pk, _kcp, _stream_type), (feedback, rendezvous_server)) =
        Client::start(peer_id, key, token, ConnType::PORT_FORWARD, interface.clone()).await?;
    let mut trust = crate::trusted_devices::client::Session::new(
        &interface.get_lch().read().unwrap(),
        stream.is_secured(),
        pk.as_deref(),
    )?;
    interface.update_direct(Some(direct));
    let _keep_it = hc_connection(feedback, rendezvous_server, token).await;
    let mut framed = Framed::new(socket, BytesCodec::new());
    let mut prebuf = Vec::new();
    loop {
        tokio::select! {
            res = timeout(READ_TIMEOUT, stream.next()) => match res {
                Ok(Some(Ok(bytes))) => {
                    let msg_in = Message::parse_from_bytes(&bytes)?;
                    match msg_in.union {
                        Some(message::Union::Hash(hash)) => {
                            {
                                let lc = interface.get_lch();
                                let mut w = lc.write().unwrap();
                                w.port_forward = ("127.0.0.1".to_owned(), remote_port);
                                w.port_forward_multiplex = false;
                                w.set_hash(hash.clone());
                            }
                            if !password.is_empty() {
                                interface
                                    .handle_login_from_ui(
                                        String::new(),
                                        String::new(),
                                        password.to_owned(),
                                        true,
                                        &mut trust,
                                        &mut stream,
                                    )
                                    .await;
                            } else if !interface.handle_hash("", hash, &mut trust, &mut stream).await {
                                return Ok(());
                            }
                        }
                        Some(message::Union::LoginResponse(lr)) => {
                            match trust.respond(lr, &interface).await.union {
                                Some(login_response::Union::PeerInfo(pi)) => {
                                    interface.handle_peer_info(pi);
                                    break;
                                }
                                Some(login_response::Union::Error(_)) => return Ok(()),
                                _ => {}
                            }
                        }
                        Some(message::Union::TestDelay(t)) => {
                            interface.handle_test_delay(t, &mut stream).await;
                        }
                        _ => {}
                    }
                }
                _ => return Ok(()),
            },
            res = framed.next() => match res {
                Some(Ok(bytes)) => prebuf.extend_from_slice(&bytes),
                _ => return Ok(()),
            },
        }
    }
    stream.set_raw();
    if !prebuf.is_empty() {
        allow_err!(stream.send_bytes(prebuf.into()).await);
    }
    loop {
        tokio::select! {
            res = framed.next() => match res {
                Some(Ok(bytes)) => allow_err!(stream.send_bytes(bytes.into()).await),
                _ => break,
            },
            res = stream.next() => match res {
                Some(Ok(bytes)) => allow_err!(framed.send(bytes).await),
                _ => break,
            },
        }
    }
    Ok(())
}

struct ReverseEntry {
    inbound: mpsc::UnboundedSender<Inbound>,
    credit: Arc<SendCredit>,
    window: Arc<Mutex<RecvWindow>>,
    opened: bool,
}

pub struct ReverseLanPeer {
    sink: FrameSink,
    teardown: watch::Receiver<bool>,
    channels: Mutex<HashMap<i32, ReverseEntry>>,
    next_id: AtomicI32,
    bound_ports: Mutex<HashMap<u16, watch::Sender<bool>>>,
}

pub fn is_reverse_channel_frame(frame: &PortForwardChannel) -> bool {
    match &frame.union {
        Some(port_forward_channel::Union::Opened(o)) => o.channel_id < 0,
        Some(port_forward_channel::Union::Data(d)) => d.channel_id < 0,
        Some(port_forward_channel::Union::Close(c)) => c.channel_id < 0,
        Some(port_forward_channel::Union::WindowUpdate(u)) => u.channel_id < 0,
        _ => false,
    }
}

impl ReverseLanPeer {
    pub fn new(sink: FrameSink, teardown: watch::Receiver<bool>) -> Arc<Self> {
        Arc::new(Self {
            sink,
            teardown,
            channels: Mutex::new(HashMap::new()),
            next_id: AtomicI32::new(-1),
            bound_ports: Mutex::new(HashMap::new()),
        })
    }

    pub fn handle_probe(self: &Arc<Self>, probe: &str) -> String {
        if let Some(rest) = probe.strip_prefix("__lan_update__:") {
            handle_incoming_update_meta(rest, true);
        } else if probe == "__lan_update__" {
            trigger_hot_update_check(true);
        } else if let Some(rest) = probe.strip_prefix("__lan_ports__:") {
            let (ports_section, meta_section) = rest.split_once(';').unwrap_or((rest, ""));
            if !meta_section.is_empty() {
                handle_incoming_update_meta(meta_section, false);
            }
            if let Some((controller_ip, ports_csv)) = ports_section.split_once(':') {
                if controller_ip.starts_with("10.144.")
                    && ensure_local_ip_alias(controller_ip, false).is_ok()
                {
                    let ports: Vec<u16> = ports_csv
                        .split(',')
                        .filter_map(|s| s.trim().parse::<u16>().ok())
                        .filter(|&p| p > 0)
                        .collect();
                    self.bind_reverse_ports(controller_ip, ports);
                }
            }
        }
        format!(
            "__lan_ports__:{},;build={};updating={}",
            get_local_listening_ports_csv(),
            get_local_build_number(),
            if IS_HOT_UPDATING.load(Ordering::Relaxed) { 1 } else { 0 }
        )
    }

    fn bind_reverse_ports(self: &Arc<Self>, controller_ip: &str, ports: Vec<u16>) {
        let mut bound = self.bound_ports.lock().unwrap();
        for port in ports {
            if bound.contains_key(&port) {
                continue;
            }
            let ip_owned = controller_ip.to_owned();
            let peer = self.clone();
            let (stop_tx, mut stop_rx) = watch::channel(false);
            bound.insert(port, stop_tx);
            let mut teardown = self.teardown.clone();
            tokio::spawn(async move {
                let Ok(listener) = bind_lan_port(&ip_owned, port).await else {
                    return;
                };
                sync_macos_pf_rules();
                loop {
                    tokio::select! {
                        _ = stop_rx.changed() => break,
                        _ = async { teardown.wait_for(|down| *down).await.is_ok() } => break,
                        res = listener.accept() => match res {
                            Ok((socket, _)) => {
                                let _ = peer.open_reverse_channel(port as i32, socket);
                            }
                            Err(_) => break,
                        }
                    }
                }
            });
        }
    }

    fn open_reverse_channel(self: &Arc<Self>, port: i32, socket: TcpStream) -> ResultType<()> {
        if self.sink.is_closed() {
            hbb_common::bail!("reverse tunnel gone");
        }
        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        let credit = Arc::new(SendCredit::new(INITIAL_WINDOW));
        let window = Arc::new(Mutex::new(RecvWindow::new(CHANNEL_WINDOW)));
        let id = {
            let mut channels = self.channels.lock().unwrap();
            if channels.len() >= MAX_CHANNELS {
                hbb_common::bail!("too many reverse channels");
            }
            let mut id = self.next_id.fetch_sub(1, Ordering::Relaxed);
            while id >= 0 || channels.contains_key(&id) {
                id = self.next_id.fetch_sub(1, Ordering::Relaxed);
            }
            channels.insert(
                id,
                ReverseEntry {
                    inbound: inbound_tx,
                    credit: credit.clone(),
                    window: window.clone(),
                    opened: false,
                },
            );
            id
        };
        let open = open_msg(id, "127.0.0.1", port, CHANNEL_WINDOW);
        let (reader, writer) = socket.into_split();
        let sink = self.sink.clone();
        let teardown = self.teardown.clone();
        let peer = self.clone();
        tokio::spawn(async move {
            if sink.send_ordered(open).await.is_ok() {
                run_channel(
                    id,
                    reader,
                    writer,
                    Vec::new(),
                    Vec::new(),
                    credit,
                    window,
                    inbound_rx,
                    sink,
                    teardown,
                )
                .await;
            }
            peer.channels.lock().unwrap().remove(&id);
        });
        Ok(())
    }

    pub fn on_frame(&self, ch: PortForwardChannel) {
        match ch.union {
            Some(port_forward_channel::Union::Opened(o)) => {
                let mut channels = self.channels.lock().unwrap();
                if o.success {
                    if let Some(e) = channels.get_mut(&o.channel_id) {
                        if !e.opened {
                            e.opened = true;
                            e.credit.raise_initial(o.window);
                        }
                    }
                } else if let Some(e) = channels.remove(&o.channel_id) {
                    e.inbound.send(Inbound::Close).ok();
                }
            }
            Some(port_forward_channel::Union::Data(d)) => {
                let mut channels = self.channels.lock().unwrap();
                let Some(e) = channels.get(&d.channel_id) else {
                    return;
                };
                let accepted = e.window.lock().unwrap().accept(d.data.len());
                let delivered = accepted && e.inbound.send(Inbound::Data(d.data)).is_ok();
                if !delivered {
                    if let Some(e) = channels.remove(&d.channel_id) {
                        if !accepted {
                            e.inbound.send(Inbound::Violation).ok();
                        }
                    }
                }
            }
            Some(port_forward_channel::Union::Close(c)) => {
                if let Some(e) = self.channels.lock().unwrap().remove(&c.channel_id) {
                    e.inbound.send(Inbound::Close).ok();
                }
            }
            Some(port_forward_channel::Union::WindowUpdate(u)) => {
                if let Some(e) = self.channels.lock().unwrap().get(&u.channel_id) {
                    e.credit.add(u.add);
                }
            }
            _ => {}
        }
    }
}

pub fn get_current_app_bundle() -> Option<std::path::PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let mut cur = exe.as_path();
    while let Some(parent) = cur.parent() {
        if parent.extension().and_then(|e| e.to_str()) == Some("app") {
            return Some(parent.to_path_buf());
        }
        cur = parent;
    }
    None
}

pub fn get_local_build_number() -> u64 {
    if let Some(bundle) = get_current_app_bundle() {
        let plist = bundle.join("Contents/Info.plist");
        if let Ok(content) = std::fs::read_to_string(&plist) {
            if let Some(idx) = content.find("<key>CFBundleVersion</key>") {
                let rest = &content[idx..];
                if let (Some(s), Some(e)) = (rest.find("<string>"), rest.find("</string>")) {
                    if s + 8 <= e {
                        if let Ok(b) = rest[s + 8..e].trim().parse::<u64>() {
                            return b.max(HOT_UPDATE_BUILD);
                        }
                    }
                }
            }
        }
    }
    HOT_UPDATE_BUILD
}

fn set_hot_update_status(state: &str, message: String, remote_ver: Option<&str>, remote_build: Option<u64>, synced: Option<usize>) {
    let mut st = HOT_UPDATE_STATUS.lock().unwrap();
    st.local_version = HOT_UPDATE_VERSION.to_owned();
    st.local_build = get_local_build_number();
    st.state = state.to_owned();
    st.message = message;
    if let Some(v) = remote_ver {
        if !v.is_empty() {
            st.remote_version = v.to_owned();
        }
    }
    if let Some(b) = remote_build {
        if b > 0 {
            st.remote_build = b;
        }
    }
    if let Some(s) = synced {
        st.synced_peers = s;
    }
}

pub fn get_hot_update_status_json() -> String {
    ensure_init();
    let mut st = HOT_UPDATE_STATUS.lock().unwrap();
    st.local_version = HOT_UPDATE_VERSION.to_owned();
    st.local_build = get_local_build_number();
    serde_json::to_string(&*st).unwrap_or_else(|_| "{}".to_owned())
}

fn handle_incoming_update_meta(meta: &str, explicit_push: bool) {
    let mut target_build = 0u64;
    let mut force_seq = 0u64;
    for part in meta.split([';', ':']) {
        if let Some(b) = part.strip_prefix("build=") {
            if let Ok(v) = b.trim().parse::<u64>() {
                target_build = v;
            }
        } else if let Some(f) = part.strip_prefix("force=") {
            if let Ok(v) = f.trim().parse::<u64>() {
                force_seq = v;
            }
        }
    }
    let local_build = get_local_build_number();
    if target_build > local_build {
        trigger_hot_update_check(false);
    } else if force_seq > 0 {
        let last = LAST_APPLIED_FORCE_SEQ.load(Ordering::Relaxed);
        if force_seq > last {
            LAST_APPLIED_FORCE_SEQ.store(force_seq, Ordering::Relaxed);
            trigger_hot_update_check(explicit_push);
        }
    } else if explicit_push {
        trigger_hot_update_check(false);
    }
}

pub fn start_hot_update_background_loop() {
    HOT_UPDATE_LOOP_ONCE.call_once(|| {
        std::thread::spawn(|| {
            std::thread::sleep(Duration::from_secs(6));
            loop {
                let _ = check_and_apply_hot_update(false);
                std::thread::sleep(Duration::from_secs(60));
            }
        });
    });
}

pub fn trigger_hot_update_check(force: bool) {
    std::thread::spawn(move || {
        let _ = check_and_apply_hot_update(force);
    });
}

fn fetch_remote_manifest() -> ResultType<VersionManifest> {
    let custom = LocalConfig::get_option("hot_update_manifest_url");
    let url = if custom.is_empty() {
        DEFAULT_HOT_UPDATE_MANIFEST_URL
    } else {
        custom.as_str()
    };
    let out = std::process::Command::new("/usr/bin/curl")
        .args(["-fsSL", "--connect-timeout", "6", "--max-time", "15", url])
        .output()?;
    if !out.status.success() {
        hbb_common::bail!("无法连接热更新服务器");
    }
    let manifest: VersionManifest = serde_json::from_slice(&out.stdout)?;
    Ok(manifest)
}

#[cfg(target_os = "macos")]
fn try_acquire_hotpatch_lock() -> Option<std::fs::File> {
    use std::os::{fd::AsRawFd, unix::fs::PermissionsExt};
    let path = "/tmp/rustdesk_hotpatch.lock";
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)
        .ok()?;
    let _ = file.set_permissions(std::fs::Permissions::from_mode(0o666));
    let rc = unsafe {
        hbb_common::libc::flock(
            file.as_raw_fd(),
            hbb_common::libc::LOCK_EX | hbb_common::libc::LOCK_NB,
        )
    };
    if rc == 0 {
        Some(file)
    } else {
        None
    }
}

pub fn check_and_apply_hot_update(force: bool) -> ResultType<bool> {
    if IS_HOT_UPDATING.load(Ordering::Relaxed) {
        return Ok(false);
    }
    let local_build = get_local_build_number();
    let manifest = fetch_remote_manifest()?;
    TARGET_UPDATE_BUILD.fetch_max(manifest.build, Ordering::Relaxed);
    if manifest.build < local_build || (!force && manifest.build == local_build) {
        set_hot_update_status(
            "up_to_date",
            format!("当前已是最新热更新版本 v{} (#{})", HOT_UPDATE_VERSION, local_build),
            Some(&manifest.version),
            Some(manifest.build),
            None,
        );
        return Ok(false);
    }

    #[cfg(target_os = "macos")]
    {
        let Some(_lock_guard) = try_acquire_hotpatch_lock() else {
            return Ok(false);
        };
        IS_HOT_UPDATING.store(true, Ordering::Relaxed);
        set_hot_update_status(
            "downloading",
            format!("发现新版本 v{} (#{})，正在后台静默热更新...", manifest.version, manifest.build),
            Some(&manifest.version),
            Some(manifest.build),
            None,
        );

        let url = if manifest.hotpatch_macos_arm64_url.is_empty() {
            DEFAULT_HOTPATCH_MACOS_URL.to_owned()
        } else {
            manifest.hotpatch_macos_arm64_url.clone()
        };
        let pid = std::process::id();
        let tar_path = format!("/tmp/rustdesk_hotpatch_{}.tar.gz", pid);
        let stage_dir = format!("/tmp/rustdesk_hotpatch_stage_{}", pid);
        let _ = std::fs::remove_file(&tar_path);
        let _ = std::fs::remove_dir_all(&stage_dir);

        let status = std::process::Command::new("/usr/bin/curl")
            .args([
                "-fsSL",
                "--connect-timeout",
                "10",
                "--max-time",
                "300",
                "-o",
                &tar_path,
                &url,
            ])
            .status();
        if !matches!(status, Ok(s) if s.success()) {
            IS_HOT_UPDATING.store(false, Ordering::Relaxed);
            let _ = std::fs::remove_file(&tar_path);
            set_hot_update_status("error", "热更新包下载失败".to_owned(), None, None, None);
            hbb_common::bail!("热更新包下载失败");
        }

        if !manifest.sha256.is_empty() {
            if let Ok(out) = std::process::Command::new("/usr/bin/shasum")
                .args(["-a", "256", &tar_path])
                .output()
            {
                let actual = String::from_utf8_lossy(&out.stdout)
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .to_lowercase();
                if !actual.is_empty() && actual != manifest.sha256.trim().to_lowercase() {
                    IS_HOT_UPDATING.store(false, Ordering::Relaxed);
                    let _ = std::fs::remove_file(&tar_path);
                    set_hot_update_status("error", "热更新包校验失败".to_owned(), None, None, None);
                    hbb_common::bail!("热更新包 SHA256 校验不匹配");
                }
            }
        }

        let _ = std::fs::create_dir_all(&stage_dir);
        let untar = std::process::Command::new("/usr/bin/tar")
            .args(["-xzf", &tar_path, "-C", &stage_dir])
            .status();
        let dylib_ok = std::path::Path::new(&stage_dir)
            .join("Contents/Frameworks/liblibrustdesk.dylib")
            .exists();
        if !matches!(untar, Ok(s) if s.success()) || !dylib_ok {
            IS_HOT_UPDATING.store(false, Ordering::Relaxed);
            let _ = std::fs::remove_file(&tar_path);
            let _ = std::fs::remove_dir_all(&stage_dir);
            set_hot_update_status("error", "解压热更新包失败".to_owned(), None, None, None);
            hbb_common::bail!("解压热更新包失败");
        }

        set_hot_update_status(
            "applying",
            format!("正在热替换至 v{} (#{}) 并自动重启...", manifest.version, manifest.build),
            Some(&manifest.version),
            Some(manifest.build),
            None,
        );

        let app_bundle = get_current_app_bundle()
            .unwrap_or_else(|| std::path::PathBuf::from("/Applications/RustDesk.app"))
            .to_string_lossy()
            .to_string();
        let script_path = format!("/tmp/rustdesk_apply_hotpatch_{}.sh", pid);
        let script = format!(
            r#"#!/bin/bash
sleep 1.0
STAGE_DIR="{stage_dir}"
APP_BUNDLE="{app_bundle}"
LOCK_FILE="/tmp/rustdesk_hotpatch.lock"

HAD_GUI=0
for p in $(pgrep -x RustDesk || true); do
  args=$(ps -p "$p" -o args= 2>/dev/null || true)
  if ! printf '%s\n' "$args" | grep -E "(--server|--service|--tray)" >/dev/null; then
    HAD_GUI=1
  fi
done

apply_to_bundle() {{
  local target="$1"
  if [ ! -d "$target" ]; then
    return 0
  fi
  local staged="/tmp/rustdesk_staged_$$.app"
  local backup="/tmp/rustdesk_backup_$$.app"
  rm -rf "$staged" "$backup"
  /usr/bin/ditto "$target" "$staged" 2>/dev/null || cp -R "$target" "$staged"
  /usr/bin/ditto "$STAGE_DIR/Contents" "$staged/Contents" 2>/dev/null || cp -Rf "$STAGE_DIR/Contents/"* "$staged/Contents/"
  rm -f "$staged/Contents/MacOS/service" 2>/dev/null || true
  /usr/bin/xattr -cr "$staged" 2>/dev/null || true
  /usr/bin/codesign --force --deep --sign - "$staged" 2>/dev/null || true

  pkill -9 -x RustDesk 2>/dev/null || sudo -n /usr/bin/pkill -9 -x RustDesk 2>/dev/null || true
  sleep 0.4

  if mv "$target" "$backup" 2>/dev/null && mv "$staged" "$target" 2>/dev/null; then
    rm -rf "$backup"
  elif sudo -n /bin/mv "$target" "$backup" 2>/dev/null && sudo -n /bin/mv "$staged" "$target" 2>/dev/null; then
    sudo -n /bin/rm -rf "$backup" 2>/dev/null || true
  else
    [ -d "$backup" ] && [ ! -d "$target" ] && (mv "$backup" "$target" 2>/dev/null || sudo -n /bin/mv "$backup" "$target" 2>/dev/null)
    rm -f "$target/Contents/MacOS/service" 2>/dev/null || sudo -n /bin/rm -f "$target/Contents/MacOS/service" 2>/dev/null || true
    /usr/bin/ditto "$staged" "$target" 2>/dev/null || sudo -n /usr/bin/ditto "$staged" "$target" 2>/dev/null || true
    /usr/bin/xattr -cr "$target" 2>/dev/null || true
    /usr/bin/codesign --force --deep --sign - "$target" 2>/dev/null || true
    rm -rf "$staged" "$backup"
  fi
}}

apply_to_bundle "$APP_BUNDLE"
if [ "$APP_BUNDLE" != "/Applications/RustDesk.app" ] && [ -d "/Applications/RustDesk.app" ]; then
  apply_to_bundle "/Applications/RustDesk.app"
fi

rm -rf "$STAGE_DIR" "{tar_path}"
rm -f "$LOCK_FILE"

if [ "$HAD_GUI" -eq 1 ]; then
  open -a "$APP_BUNDLE" 2>/dev/null || "$APP_BUNDLE/Contents/MacOS/RustDesk" >/dev/null 2>&1 &
else
  "$APP_BUNDLE/Contents/MacOS/RustDesk" --server >/dev/null 2>&1 &
fi
rm -f "$0"
"#
        );

        std::fs::write(&script_path, script.as_bytes())?;
        let _ = std::process::Command::new("/bin/chmod")
            .args(["+x", &script_path])
            .status();
        {
            use std::os::unix::process::CommandExt;
            let _ = std::process::Command::new("/bin/bash")
                .arg(&script_path)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .process_group(0)
                .spawn();
        }
        return Ok(true);
    }

    #[cfg(not(target_os = "macos"))]
    {
        Ok(false)
    }
}

#[cfg(target_os = "macos")]
fn try_publish_local_bundle_if_newer(remote_build: u64) -> Option<u64> {
    let local_build = get_local_build_number();
    if local_build <= remote_build {
        return None;
    }
    let bundle = get_current_app_bundle()?;
    let ssh_ok = std::process::Command::new("/usr/bin/ssh")
        .args(["-o", "BatchMode=yes", "-o", "ConnectTimeout=4", "root@124.222.82.53", "true"])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if !ssh_ok {
        return None;
    }
    set_hot_update_status(
        "publishing",
        format!("正在将本机版本 v{} (#{}) 同步上传至热更新中心...", HOT_UPDATE_VERSION, local_build),
        Some(HOT_UPDATE_VERSION),
        Some(local_build),
        None,
    );
    let pid = std::process::id();
    let tar_path = format!("/tmp/hotpatch-publish-{}.tar.gz", pid);
    let manifest_path = format!("/tmp/version-publish-{}.json", pid);
    let bundle_str = bundle.to_string_lossy().to_string();
    let tar_ok = std::process::Command::new("/usr/bin/tar")
        .args([
            "-czf",
            &tar_path,
            "-C",
            &bundle_str,
            "Contents/Info.plist",
            "Contents/MacOS/RustDesk",
            "Contents/Frameworks/App.framework",
            "Contents/Frameworks/liblibrustdesk.dylib",
        ])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if !tar_ok {
        let _ = std::fs::remove_file(&tar_path);
        return None;
    }
    let sha256 = std::process::Command::new("/usr/bin/shasum")
        .args(["-a", "256", &tar_path])
        .output()
        .ok()
        .and_then(|o| {
            String::from_utf8_lossy(&o.stdout)
                .split_whitespace()
                .next()
                .map(|s| s.to_lowercase())
        })
        .unwrap_or_default();
    let manifest = VersionManifest {
        version: HOT_UPDATE_VERSION.to_owned(),
        build: local_build,
        hotpatch_macos_arm64_url: DEFAULT_HOTPATCH_MACOS_URL.to_owned(),
        dmg_macos_arm64_url: "http://124.222.82.53/RustDesk.dmg".to_owned(),
        sha256,
        notes: "同步热更新版本".to_owned(),
    };
    if let Ok(json) = serde_json::to_string_pretty(&manifest) {
        let _ = std::fs::write(&manifest_path, json.as_bytes());
        let scp_ok = std::process::Command::new("/usr/bin/scp")
            .args([
                "-q",
                "-o",
                "BatchMode=yes",
                &tar_path,
                "root@124.222.82.53:/var/www/app-shop/official-website/dist/downloads/hotpatch-macos-arm64.tar.gz",
            ])
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
            && std::process::Command::new("/usr/bin/scp")
                .args([
                    "-q",
                    "-o",
                    "BatchMode=yes",
                    &manifest_path,
                    "root@124.222.82.53:/var/www/app-shop/official-website/dist/downloads/version.json",
                ])
                .status()
                .map(|s| s.success())
                .unwrap_or(false);
        let _ = std::fs::remove_file(&tar_path);
        let _ = std::fs::remove_file(&manifest_path);
        if scp_ok {
            return Some(local_build);
        }
    }
    let _ = std::fs::remove_file(&tar_path);
    let _ = std::fs::remove_file(&manifest_path);
    None
}

#[derive(Deserialize, Default)]
struct HotUpdateSyncRequest {
    #[serde(default)]
    peers: Vec<String>,
    #[serde(default)]
    force_self: bool,
}

pub fn handle_hot_update_sync(json: &str) {
    ensure_init();
    let req: HotUpdateSyncRequest = serde_json::from_str(json).unwrap_or_default();
    std::thread::spawn(move || {
        set_hot_update_status("syncing", "正在检查热更新服务器并向所有客户端推送同步指令...".to_owned(), None, None, None);
        let local_build = get_local_build_number();
        let mut remote_build = local_build;
        let mut remote_ver = HOT_UPDATE_VERSION.to_owned();
        if let Ok(m) = fetch_remote_manifest() {
            remote_build = m.build;
            if !m.version.is_empty() {
                remote_ver = m.version;
            }
        }
        #[cfg(target_os = "macos")]
        if let Some(published_build) = try_publish_local_bundle_if_newer(remote_build) {
            remote_build = published_build;
        }

        let target_build = remote_build.max(local_build);
        let now_seq = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(1);
        TARGET_UPDATE_BUILD.store(target_build, Ordering::Relaxed);
        FORCE_UPDATE_SEQ.store(now_seq, Ordering::Relaxed);

        let active_handles: Vec<(String, Arc<TunnelHandle>)> = {
            let map = ACTIVE_TUNNELS.lock().unwrap();
            map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };
        let active_ids: HashSet<String> = active_handles.iter().map(|(k, _)| k.clone()).collect();

        let mut peer_candidates: HashSet<String> = load_enabled_peers()
            .into_iter()
            .map(|id| clean_peer_id(&id))
            .collect();
        for id in req.peers {
            let c = clean_peer_id(&id);
            if !c.is_empty() {
                peer_candidates.insert(c);
            }
        }

        let mut total_notified = active_handles.len();
        let push_cmd = format!("__lan_update__:build={};force={}", target_build, now_seq);
        if let Ok(rt) = tokio::runtime::Builder::new_Multi_thread_or_current() {
            rt.block_on(async {
                for (_, h) in &active_handles {
                    let _ = h.probe_remote_ports(&push_cmd).await;
                }
            });
        }

        for peer_id in peer_candidates {
            if active_ids.contains(&peer_id) || !peer_has_saved_password(&peer_id) {
                continue;
            }
            total_notified += 1;
            let cmd = push_cmd.clone();
            std::thread::spawn(move || {
                if let Ok(rt) = tokio::runtime::Builder::new_Multi_thread_or_current() {
                    let _ = rt.block_on(push_oneshot_update_to_peer(&peer_id, &cmd));
                }
            });
        }

        if remote_build > local_build || req.force_self {
            let _ = check_and_apply_hot_update(req.force_self);
        } else {
            set_hot_update_status(
                "up_to_date",
                format!(
                    "已向 {} 台客户端发送同步热更新指令 (目标版本 v{} #{})",
                    total_notified, remote_ver, target_build
                ),
                Some(&remote_ver),
                Some(target_build),
                Some(total_notified),
            );
        }
    });
}

async fn push_oneshot_update_to_peer(peer_id: &str, push_cmd: &str) -> ResultType<()> {
    let key = crate::get_key(false).await;
    let token = LocalConfig::get_option("access_token");
    let clean_id = clean_peer_id(peer_id);
    let mut lc = LoginConfigHandler::default();
    lc.initialize(
        peer_id.to_owned(),
        ConnType::PORT_FORWARD,
        None,
        false,
        None,
        None,
        None,
    );
    lc.port_forward = ("LAN".to_owned(), 0);
    lc.port_forward_multiplex = true;
    lc.port_forward_mux = true;
    let iface = LanClientInterface {
        lc: Arc::new(RwLock::new(lc)),
        clean_id,
        need_password: Arc::new(Mutex::new(false)),
        last_error: Arc::new(Mutex::new(String::new())),
    };
    let (_stop_tx, mut stop_rx) = watch::channel(false);
    let Some(LanTunnelConn::Stream(stream, mux)) =
        connect_lan_tunnel(peer_id, "", &key, &token, iface.clone(), &mut stop_rx).await?
    else {
        return Ok(());
    };
    if mux {
        let tunnel = Tunnel::new();
        let _claim = tunnel.claim();
        let handle = tunnel.set_muxed(stream, iface);
        let _ = handle.probe_remote_ports(push_cmd).await;
        sleep(Duration::from_millis(1200)).await;
    }
    Ok(())
}
