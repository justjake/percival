#!/usr/bin/bash

set -eo pipefail

if rustup -V ; then
  echo "Rust already installed"
else
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
fi
./build-percival-wasm-with-typescript-types.sh
