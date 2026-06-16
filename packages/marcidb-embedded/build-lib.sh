#!/bin/bash
# Builds the marcidb-ffi native library (full variant: vector + fulltext, needs nightly) for one platform
# and stages it under packages/marcidb-embedded/native/ with the name the loader expects.
#
# Usage: ./build-lib.sh [linux|mac|win]
set -e

PLATFORM=$1
if [ -z "$PLATFORM" ]; then
  echo "Usage: ./build-lib.sh [linux|mac|win]"
  exit 1
fi

# The full variant pulls in the vector module, which needs nightly (portable_simd).
TOOLCHAIN="+nightly"

case "$PLATFORM" in
  linux)
    TARGET="x86_64-unknown-linux-gnu"
    SUFFIX="linux-x64"
    SRC="libmarcidb_ffi.so"
    DST_EXT="so"
    ;;
  mac)
    TARGET="aarch64-apple-darwin"
    SUFFIX="darwin-arm64"
    SRC="libmarcidb_ffi.dylib"
    DST_EXT="dylib"
    ;;
  win)
    TARGET="x86_64-pc-windows-msvc"
    SUFFIX="win32-x64"
    SRC="marcidb_ffi.dll"
    DST_EXT="dll"
    ;;
  *)
    echo "Unknown platform: $PLATFORM"
    exit 1
    ;;
esac

echo "Building marcidb-ffi (full) for $PLATFORM ($TARGET)..."
cargo $TOOLCHAIN build -p marcidb-ffi --release --target "$TARGET"

mkdir -p packages/marcidb-embedded/native
SRC_PATH="target/$TARGET/release/$SRC"
DST_PATH="packages/marcidb-embedded/native/marcidb-$SUFFIX.$DST_EXT"
echo "Copying $SRC_PATH -> $DST_PATH"
cp "$SRC_PATH" "$DST_PATH"
echo "Done: $DST_PATH"
