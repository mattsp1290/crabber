#!/bin/sh
set -eu
cd "$(dirname "$0")"
cargo build -p external-guest -p external-controls --target wasm32-wasip2 --release
cargo run -p external-host
