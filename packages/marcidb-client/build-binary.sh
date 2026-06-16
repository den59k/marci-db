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
    SUFFIX="linux-x64"
    EXT=""
    ;;
  mac)
    TARGET="aarch64-apple-darwin"
    SUFFIX="darwin-arm64"
    EXT=""
    ;;
  win)
    TARGET="x86_64-pc-windows-msvc"
    SUFFIX="win32-x64"
    EXT=".exe"
    ;;
  *)
    echo "Unknown platform: $PLATFORM"
    echo "Usage: ./build.sh [linux|mac|win]"
    exit 1
    ;;
esac

# Два бинаря: marcidb-ts → marci-generate (TS-типы), marcidb-migrate → marci-migrate (миграции).
# (имя бинаря крейта marcidb-migrate переопределено на marci-migrate в его Cargo.toml)
echo "Building for $PLATFORM ($TARGET)..."
cargo build -p marcidb-ts -p marcidb-migrate --release --target "$TARGET"

mkdir -p packages/npm/bin

copy_bin() {
  local src="target/$TARGET/release/$1$EXT"
  local dst="packages/npm/bin/$2-$SUFFIX$EXT"
  echo "Copying $src -> $dst..."
  cp "$src" "$dst"
}

copy_bin "marcidb-ts" "marci-generate"
copy_bin "marci-migrate" "marci-migrate"

echo "Done: packages/npm/bin/marci-generate-$SUFFIX$EXT, packages/npm/bin/marci-migrate-$SUFFIX$EXT"
