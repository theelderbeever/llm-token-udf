#!/usr/bin/env bash
# Match the Native runtime bundle layout of count_tokens_rs/build.sh.
set -euo pipefail
cd "$(dirname "$0")"

for target in x86_64-unknown-linux-musl aarch64-unknown-linux-musl; do
    cargo build --locked --release --target "$target"
done

rm -rf dist
mkdir -p dist/amd64 dist/arm64
cp target/x86_64-unknown-linux-musl/release/count_token_chrs dist/amd64/main
cp target/aarch64-unknown-linux-musl/release/count_token_chrs dist/arm64/main
cp ../count_tokens_rs/models.json dist/amd64/
cp ../count_tokens_rs/models.json dist/arm64/
# Recreate the archive so removed files cannot survive a rebuild.
rm -f count_token_chrs.zip
(cd dist && zip -r ../count_token_chrs.zip amd64 arm64)
