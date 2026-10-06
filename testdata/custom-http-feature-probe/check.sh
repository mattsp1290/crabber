#!/bin/sh
set -eu

cd "$(dirname "$0")"
ROOT=$(cd ../.. && pwd)
TMP="$PWD/.probe-tmp"
OUT="$TMP/output.json"

rm -rf "$TMP"
mkdir -p "$TMP/src"
trap 'rm -rf "$TMP"' EXIT HUP INT TERM

cargo check --quiet

graph=$(cargo tree -e features)
printf '%s\n' "$graph" | grep -q 'crabber-providers feature "custom-http"'
if printf '%s\n' "$graph" | grep -Eq 'crabber-auth|crabber-providers feature "(anthropic|openai|codex|opencode-go|all-providers)"'; then
    echo "custom-http unexpectedly enabled a built-in provider or crabber-auth" >&2
    exit 1
fi

# Compile one forbidden API per crate and assert only that compilation fails.
# Public-surface probes deliberately avoid rustc error codes and rendered text,
# which are not stable across the repository's moving stable toolchain.
expect_compile_failure() {
    label=$1
    features=$2
    source=$3
    cat > "$TMP/Cargo.toml" <<EOF
[package]
name = "feature-gate-negative-probe"
version = "0.0.0"
edition = "2024"
publish = false

[workspace]

[dependencies]
crabber-providers = { path = "$ROOT/crates/crabber-providers", default-features = false, features = [$features] }
reqwest = { version = "0.12", default-features = false, features = ["rustls-tls"] }
EOF
    printf '%s\n' "$source" > "$TMP/src/main.rs"
    if CARGO_TARGET_DIR="$PWD/target" cargo check --quiet --manifest-path "$TMP/Cargo.toml" >"$OUT" 2>&1; then
        echo "$label unexpectedly compiled" >&2
        exit 1
    fi
}

# custom-http alone must not expose any built-in constructor. Keep each use
# isolated so one genuine error cannot hide an accidentally exposed sibling.
expect_compile_failure anthropic_constructor '"custom-http"' '
use crabber_providers::HttpAdapter;
fn main() { let _ = HttpAdapter::anthropic(); }
'
expect_compile_failure openai_constructor '"custom-http"' '
use crabber_providers::HttpAdapter;
fn main() { let _ = HttpAdapter::openai(); }
'
expect_compile_failure codex_constructor '"custom-http"' '
use crabber_providers::HttpAdapter;
fn main() { let _ = HttpAdapter::codex(todo!()); }
'
expect_compile_failure opencode_go_constructor '"custom-http"' '
use crabber_providers::{HttpAdapter, Protocol};
fn main() { let _ = HttpAdapter::opencode_go(Protocol::Responses); }
'

# all-providers must not expose custom-http symbols or its constructor.
expect_compile_failure auth_scheme '"all-providers"' 'use crabber_providers::AuthScheme; fn main() {}'
expect_compile_failure token_field '"all-providers"' 'use crabber_providers::ChatTokenField; fn main() {}'
expect_compile_failure credential_source '"all-providers"' 'use crabber_providers::CredentialSource; fn main() {}'
expect_compile_failure header_hook '"all-providers"' 'use crabber_providers::RequestHeaderHook; fn main() {}'
expect_compile_failure response_observer '"all-providers"' 'use crabber_providers::ResponseObserver; fn main() {}'
expect_compile_failure error_classifier '"all-providers"' 'use crabber_providers::ErrorClassifier; fn main() {}'
expect_compile_failure client_config '"all-providers"' 'use crabber_providers::HttpClientConfig; fn main() {}'
expect_compile_failure proxy_config '"all-providers"' 'use crabber_providers::HttpProxyConfig; fn main() {}'
expect_compile_failure custom_constructor '"all-providers"' '
use crabber_providers::{HttpAdapter, Protocol};
fn main() { let _ = HttpAdapter::custom("x", "https://example.test", Protocol::Responses); }
'

# In mixed-feature builds, built-in adapters must not expose any custom-only
# setting. Each compile-fail fixture covers one method on a built-in value.
expect_compile_failure mixed_auth '"all-providers", "custom-http"' '
use crabber_providers::{AuthScheme, HttpAdapter};
fn main() { let _ = HttpAdapter::openai().with_auth_scheme(AuthScheme::XApiKey); }
'
expect_compile_failure mixed_credential_source '"all-providers", "custom-http"' '
use crabber_providers::HttpAdapter;
fn main() { let _ = HttpAdapter::openai().with_credential_source(todo!()); }
'
expect_compile_failure mixed_static_headers '"all-providers", "custom-http"' '
use crabber_providers::HttpAdapter;
fn main() { let _ = HttpAdapter::openai().try_with_static_headers(reqwest::header::HeaderMap::new()); }
'
expect_compile_failure mixed_header_hook '"all-providers", "custom-http"' '
use crabber_providers::HttpAdapter;
fn main() { let _ = HttpAdapter::openai().with_request_header_hook(todo!()); }
'
expect_compile_failure mixed_observer '"all-providers", "custom-http"' '
use crabber_providers::HttpAdapter;
fn main() { let _ = HttpAdapter::openai().with_response_observer(todo!()); }
'
expect_compile_failure mixed_classifier '"all-providers", "custom-http"' '
use crabber_providers::HttpAdapter;
fn main() { let _ = HttpAdapter::openai().with_error_classifier(todo!()); }
'
expect_compile_failure mixed_client_config '"all-providers", "custom-http"' '
use crabber_providers::{HttpAdapter, HttpClientConfig};
fn main() { let _ = HttpAdapter::openai().try_with_client_config(HttpClientConfig::new()); }
'
expect_compile_failure mixed_chat_token '"all-providers", "custom-http"' '
use crabber_providers::{ChatTokenField, HttpAdapter};
fn main() { let _ = HttpAdapter::openai().with_chat_token_field(ChatTokenField::MaxTokens); }
'

# The custom surface must not provide raw-client escape hatches. Each method
# and conversion is isolated and checked against its specific rustc error.
expect_compile_failure with_client '"custom-http"' '
use crabber_providers::{HttpAdapter, Protocol};
fn main() { let _ = HttpAdapter::custom("x", "https://example.test", Protocol::Responses).with_client(reqwest::Client::new()); }
'
expect_compile_failure client_conversion '"custom-http"' '
use crabber_providers::HttpClientConfig;
fn main() { let _: HttpClientConfig = reqwest::Client::new().into(); }
'
expect_compile_failure builder_conversion '"custom-http"' '
use crabber_providers::HttpClientConfig;
fn main() { let _: HttpClientConfig = reqwest::Client::builder().into(); }
'
expect_compile_failure proxy_conversion '"custom-http"' '
use crabber_providers::HttpProxyConfig;
fn main() { let _: HttpProxyConfig = reqwest::Proxy::all("http://localhost:8080").unwrap().into(); }
'
expect_compile_failure default_headers '"custom-http"' '
use crabber_providers::HttpClientConfig;
fn main() { let _ = HttpClientConfig::new().default_headers(reqwest::header::HeaderMap::new()); }
'
expect_compile_failure redirect '"custom-http"' '
use crabber_providers::HttpClientConfig;
fn main() { let _ = HttpClientConfig::new().redirect(reqwest::redirect::Policy::none()); }
'
expect_compile_failure configure '"custom-http"' '
use crabber_providers::HttpClientConfig;
fn main() { let _ = HttpClientConfig::new().configure(|builder| builder); }
'

echo "custom-http feature probe passed"