#!/bin/sh
set -eu

cd "$(dirname "$0")"
python3 -I -S test_diagnostic.py
probe_workspace=$(cd ../.. && pwd)
probe_cache=$(python3 -I -S - "$probe_workspace" <<'CACHE'
import hashlib, os, sys
from pathlib import Path
root = Path(sys.argv[1]).resolve()
target = Path(os.environ.get("CARGO_TARGET_DIR", str(root / "target")))
if not target.is_absolute():
    target = root / target
identity = hashlib.sha256(os.fsencode(root)).hexdigest()[:32]
print(target / "consumer-probes" / identity / "custom-http")
CACHE
)
mkdir -p "$probe_cache"
# Serialize complete refresh/build histories, including writes outside Cargo's
# own target lock. Unique package identity also prevents cross-scratch reuse of
# a successful positive probe when another source has an older timestamp.
command -v flock >/dev/null 2>&1 || { echo "install flock for serialized feature probes" >&2; exit 1; }
exec 9>"$probe_cache/probe.lock"
flock 9
probe_identity=$(python3 -I -S -c 'import uuid; print(uuid.uuid4().hex)')
probe_target="$probe_cache/target"
probe_scratch=$(mktemp -d "$probe_cache/scratch.XXXXXXXX")
mkdir "$probe_scratch/src"
probe_output="$probe_scratch/output.json"
probe_stderr="$probe_scratch/stderr.log"
trap 'rm -rf "$probe_scratch"' EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

CARGO_TARGET_DIR="$probe_target" cargo check --quiet

graph=$(cargo tree -e features)
printf '%s\n' "$graph" | grep -q 'crabber-providers feature "custom-http"'
if printf '%s\n' "$graph" | grep -Eq 'crabber-auth|crabber-providers feature "(anthropic|openai|codex|opencode-go|all-providers)"'; then
    echo "custom-http unexpectedly enabled a built-in provider or crabber-auth" >&2
    exit 1
fi

# Each mutable manifest belongs to this invocation's unique scratch directory;
# its writes and Cargo calls are sequential. Cargo locks the persistent target.
expect_compile_failure() {
    label=$1
    features=$2
    source=$3
    case "$label" in
        *_conversion) expected_code=E0277 ;;
        auth_scheme|token_field|credential_source|header_hook|response_observer|error_classifier|client_config|proxy_config) expected_code=E0432 ;;
        *) expected_code=E0599 ;;
    esac
    cat > "$probe_scratch/Cargo.toml" <<MANIFEST
[package]
name = "feature-gate-negative-probe-$probe_identity"
version = "0.0.0"
edition = "2024"
publish = false

[workspace]

[dependencies]
crabber-providers = { path = "$probe_workspace/crates/crabber-providers", default-features = false, features = [$features] }
reqwest = { version = "0.12", default-features = false, features = ["rustls-tls"] }
MANIFEST
    cp "$probe_workspace/Cargo.lock" "$probe_scratch/Cargo.lock"
    case "$features" in
        '"custom-http"') positive_source='
use crabber_providers::{HttpAdapter, HttpClientConfig, HttpProxyConfig, Protocol};
fn main() {
    let _ = HttpAdapter::custom("probe", "https://example.test", Protocol::Responses);
    let _ = HttpClientConfig::new();
    let _: Option<HttpProxyConfig> = None;
}' ;;
        *) positive_source='use crabber_providers::HttpAdapter; fn main() { let _ = HttpAdapter::openai(); }' ;;
    esac
    printf '%s\n' "$positive_source" > "$probe_scratch/src/main.rs"
    # Resolve and compile this exact feature graph successfully before accepting
    # any negative result. This also makes dependency failures explicit.
    if ! CARGO_TARGET_DIR="$probe_target" cargo check --quiet --message-format=json --manifest-path "$probe_scratch/Cargo.toml" >"$probe_output" 2>"$probe_stderr"; then
        echo "$label positive feature preflight failed" >&2
        cat "$probe_stderr" >&2
        exit 1
    fi
    printf '%s\n' "$source" > "$probe_scratch/src/main.rs"
    if CARGO_TARGET_DIR="$probe_target" cargo check --quiet --message-format=json --manifest-path "$probe_scratch/Cargo.toml" >"$probe_output" 2>"$probe_stderr"; then
        echo "$label unexpectedly compiled" >&2
        cat "$probe_output" "$probe_stderr" >&2
        exit 1
    fi
    if ! python3 -I -S check-diagnostic.py "$probe_output" "$probe_scratch/src/main.rs" "$expected_code"; then
        echo "$label failed without its expected root-crate diagnostic ($expected_code)" >&2
        cat "$probe_output" "$probe_stderr" >&2
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
