#!/bin/bash
set -e

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_DIR="$ROOT_DIR/flutter/build/macos/Build/Products/Release/RustDesk.app"
PLIST="$APP_DIR/Contents/Info.plist"

CUR_BUILD=$(/usr/libexec/PlistBuddy -c "Print :CFBundleVersion" "$PLIST" 2>/dev/null || echo 70)
NEW_BUILD=$((CUR_BUILD + 1))

echo "==> [1/4] 编译 Rust 核心动态库 (Build #$NEW_BUILD)..."
export VCPKG_ROOT=/Users/jun/vcpkg MACOSX_DEPLOYMENT_TARGET=12.3
cd "$ROOT_DIR"
cargo build --release --lib --no-default-features --features flutter,unix-file-copy-paste
install_name_tool -id "@rpath/liblibrustdesk.dylib" target/release/liblibrustdesk.dylib

if [ "$SKIP_FLUTTER" != "1" ] || [ ! -d "$ROOT_DIR/flutter/build/macos/Build/Products/Release/App.framework" ]; then
  echo "==> [2/4] 编译 Flutter 界面资源 (App.framework)..."
  cd "$ROOT_DIR/flutter"
  flutter assemble \
    --output=build/macos/Build/Products/Release \
    -dTargetPlatform=darwin \
    -dDarwinArchs=arm64 \
    -dBuildMode=release \
    -dTreeShakeIcons=true \
    release_macos_bundle_flutter_assets
else
  echo "==> [2/4] 复用已编译的 Flutter 界面资源 (App.framework)..."
fi

echo "==> [3/4] 组装 RustDesk.app (v1.5.1 #$NEW_BUILD) 与热更新包..."
rm -f "$APP_DIR/Contents/MacOS/service"
cp -f "$ROOT_DIR/target/release/liblibrustdesk.dylib" "$APP_DIR/Contents/Frameworks/liblibrustdesk.dylib"
rm -rf "$APP_DIR/Contents/Frameworks/App.framework"
ditto "$ROOT_DIR/flutter/build/macos/Build/Products/Release/App.framework" "$APP_DIR/Contents/Frameworks/App.framework"
/usr/libexec/PlistBuddy -c "Set :CFBundleShortVersionString 1.5.1" "$PLIST"
/usr/libexec/PlistBuddy -c "Set :CFBundleVersion $NEW_BUILD" "$PLIST"
xattr -cr "$APP_DIR"
codesign --force --deep --sign - "$APP_DIR"
codesign -vvv --deep --strict "$APP_DIR"

if [ -d "/Applications/RustDesk.app" ]; then
  rm -rf "/Applications/RustDesk.app/Contents/Frameworks" "/Applications/RustDesk.app/Contents/MacOS" "/Applications/RustDesk.app/Contents/Resources" "/Applications/RustDesk.app/Contents/Info.plist" 2>/dev/null || true
  ditto "$APP_DIR" "/Applications/RustDesk.app"
  xattr -cr "/Applications/RustDesk.app"
  codesign --force --deep --sign - "/Applications/RustDesk.app"
fi

tar -czf /Users/jun/Desktop/hotpatch-macos-arm64.tar.gz -C "$APP_DIR" \
  Contents/Info.plist \
  Contents/MacOS/RustDesk \
  Contents/Frameworks/App.framework \
  Contents/Frameworks/liblibrustdesk.dylib

SHA256=$(shasum -a 256 /Users/jun/Desktop/hotpatch-macos-arm64.tar.gz | awk '{print $1}')

cat > /Users/jun/Desktop/version.json <<EOF
{
  "version": "1.5.1",
  "build": $NEW_BUILD,
  "hotpatch_macos_arm64_url": "http://124.222.82.53/downloads/hotpatch-macos-arm64.tar.gz",
  "dmg_macos_arm64_url": "http://124.222.82.53/RustDesk.dmg",
  "sha256": "$SHA256",
  "notes": "热更新同步版本 #$NEW_BUILD"
}
EOF

STAGE_DMG="/tmp/rustdesk_dmg_stage_$NEW_BUILD"
rm -rf "$STAGE_DMG"
mkdir -p "$STAGE_DMG"
ditto "$APP_DIR" "$STAGE_DMG/RustDesk.app"
ln -s /Applications "$STAGE_DMG/Applications"
rm -f /Users/jun/Desktop/RustDesk-MultiFullscreen-macOS-arm64.dmg
hdiutil create -volname "RustDesk" -srcfolder "$STAGE_DMG" -ov -format UDZO /Users/jun/Desktop/RustDesk-MultiFullscreen-macOS-arm64.dmg
rm -rf "$STAGE_DMG"

echo "==> [4/4] 上传热更新包与 DMG 至服务器 124.222.82.53..."
scp /Users/jun/Desktop/hotpatch-macos-arm64.tar.gz \
    /Users/jun/Desktop/version.json \
    /Users/jun/Desktop/RustDesk-MultiFullscreen-macOS-arm64.dmg \
    root@124.222.82.53:/var/www/app-shop/official-website/dist/downloads/
ssh root@124.222.82.53 "cp -f /var/www/app-shop/official-website/dist/downloads/RustDesk-MultiFullscreen-macOS-arm64.dmg /var/www/app-shop/official-website/dist/RustDesk.dmg"

echo "✅ 发布完成！最新热更新版本: v1.5.1 (#$NEW_BUILD)"
echo "   - 版本清单: http://124.222.82.53/downloads/version.json"
echo "   - 热更新包: http://124.222.82.53/downloads/hotpatch-macos-arm64.tar.gz"
echo "   - 完整安装包: http://124.222.82.53/RustDesk.dmg"
