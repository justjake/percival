#!/usr/bin/bash

set -eo pipefail

if rustup -V ; then
  echo "Rust already installed"
else
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
  source "$HOME/.cargo/env"
fi

if wasm-pack -V ; then
  echo "WASM-Pack already installed"
else
  curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh
fi

./build-percival-wasm-with-typescript-types.sh
