#!/usr/bin/bash

set -eo pipefail
set -x

# Do some funny stuff
mkdir -p node_modules/.cargo
ln -s node_modules/.cargo "$HOME/.cargo"

if [[ -e "$HOME/.cargo/env" ]] ; then
  source "$HOME/.cargo/env"
fi

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
