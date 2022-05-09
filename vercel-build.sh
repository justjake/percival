#!/usr/bin/bash

set -eo pipefail
set -x

# Do some funny stuff
export RUSTUP_HOME="$PWD/node_modules/.rustup"
ls -la "$RUSTUP_HOME" || true

export CARGO_HOME="$PWD/node_modules/.cargo"
ls -la "$CARGO_HOME" || true

cargo_env_file="$CARGO_HOME/env"

if [[ -e "$cargo_env_file" ]] ; then
  echo "Have .cargo/env file!"
  source "$cargo_env_file"
fi

if rustup -V ; then
  echo "Rust already installed"
else
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
  source "$cargo_env_file"
fi

if wasm-pack -V ; then
  echo "WASM-Pack already installed"
else
  curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh
fi

./build-percival-wasm-with-typescript-types.sh
