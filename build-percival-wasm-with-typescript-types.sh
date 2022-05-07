#!/usr/bin/env bash
set -eo pipefail

# Abstracts the difference between GNU and macOS sed.
sed_regex_replace() {
  local regex="$1"
  local file="$2"

  if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i "" -e "$regex" "$file"
  else
    sed -i "$regex" "$file"
  fi
}

TYPES=crates/percival/bindings
WASM_PACKAGE=crates/percival-wasm/pkg

# Build the WASM package.
wasm-pack build --target web crates/percival-wasm

# Generate Typescript types for the AST.
cargo test ast::export_bindings

# Copy auto-generated AST types to the WASM package.
mkdir -p "$WASM_PACKAGE/ast"
for typescript_file in "$TYPES"/*.ts ; do
  dts_filename="$(basename "${typescript_file%.ts}.d.ts")"
	cp -v "$typescript_file" "$WASM_PACKAGE"/ast/"$dts_filename"
done

# Edit the wasm-pack types to return the correct AST types.
sed_regex_replace 's~ast(): any~ast(): import("./ast/Program").Program | undefined~' "$WASM_PACKAGE"/percival_wasm.d.ts
