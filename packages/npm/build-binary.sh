#!/bin/bash

set -e

PLATFORM=$1

if [ -z "$PLATFORM" ]; then
  echo "Usage: ./build.sh [linux|mac|win]"
  exit 1
fi

case "$PLATFORM" in
  linux)
    TARGET="x86_64-unknown-linux-gnu"
    BIN_SRC="target/$TARGET/release/marcidb-ts"
    BIN_DST="packages/npm/bin/marci-generate-linux-x64"
    ;;
  mac)
    TARGET="aarch64-apple-darwin"
    BIN_SRC="target/$TARGET/release/marcidb-ts"
    BIN_DST="packages/npm/bin/marci-generate-darwin-arm64"
    ;;
  win)
    TARGET="x86_64-pc-windows-msvc"
    BIN_SRC="target/$TARGET/release/marcidb-ts.exe"
    BIN_DST="packages/npm/bin/marci-generate-win32-x64.exe"
    ;;
  *)
    echo "Unknown platform: $PLATFORM"
    echo "Usage: ./build.sh [linux|mac|win]"
    exit 1
    ;;
esac

echo "Building for $PLATFORM ($TARGET)..."
cargo build -p marcidb-ts --release --target "$TARGET"

echo "Copying binary to $BIN_DST..."
mkdir -p packages/npm/bin
cp "$BIN_SRC" "$BIN_DST"

echo "Done: $BIN_DST"