#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DATA_DIR="data"
CPP_ROOT="${FASTER_CPP_ROOT:-}"

if [[ -z "$CPP_ROOT" ]]; then
  if [[ -d "../../cpp/FASTER/cc/src" ]]; then
    CPP_ROOT="../../cpp/FASTER"
  elif [[ -d "../cpp/FASTER/cc/src" ]]; then
    CPP_ROOT="../cpp/FASTER"
  else
    echo "FASTER_CPP_ROOT is not set. Point it to the FASTER C++ repo root." >&2
    exit 1
  fi
fi

CXX="${CXX:-clang++}"
if ! command -v "$CXX" >/dev/null 2>&1; then
  echo "C++ compiler not found. Set CXX to clang++ or g++." >&2
  exit 1
fi

mkdir -p "$DATA_DIR"

CPP_BIN="$DATA_DIR/faster_cpp_demo"
"$CXX" -std=c++17 -I "$CPP_ROOT/cc/src" tools/faster_cpp_demo.cpp -o "$CPP_BIN"

for dir in "$DATA_DIR/cpp_gen" "$DATA_DIR/cpp_updated" "$DATA_DIR/rust_gen" "$DATA_DIR/rust_updated"; do
  if [[ -e "$dir" ]]; then
    echo "Path exists: $dir. Remove it before running this script." >&2
    exit 1
  fi
done

"$CPP_BIN" generate --out "$DATA_DIR/cpp_gen"
cargo run --quiet --bin interop_index_tool --features clap -- verify --dir "$DATA_DIR/cpp_gen"
cargo run --quiet --bin interop_index_tool --features clap -- update --dir "$DATA_DIR/cpp_gen" \
  --out "$DATA_DIR/cpp_updated" --entries "100=4096,200=8192"
"$CPP_BIN" verify --dir "$DATA_DIR/cpp_updated"

cargo run --quiet --bin interop_index_tool --features clap -- generate --out "$DATA_DIR/rust_gen"
"$CPP_BIN" verify --dir "$DATA_DIR/rust_gen"
"$CPP_BIN" update --dir "$DATA_DIR/rust_gen" --out "$DATA_DIR/rust_updated" \
  --entries "100=4096,200=8192"
cargo run --quiet --bin interop_index_tool --features clap -- verify --dir "$DATA_DIR/rust_updated"
