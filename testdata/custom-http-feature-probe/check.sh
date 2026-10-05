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

# Compile one forbidden API per crate and validate the structured rustc diagnostic.
# This intentionally ignores rendered output, package names, children/suggestions,
# and every diagnostic unrelated to the primary span in our generated source.
expect_diagnostic() {
    label=$1
    features=$2
    code=$3
    kind=$4
    expected=$5
    source=$6
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
    if CARGO_TARGET_DIR="$PWD/target" cargo check --quiet --message-format=json --manifest-path "$TMP/Cargo.toml" >"$OUT" 2>&1; then
        echo "$label unexpectedly compiled" >&2
        exit 1
    fi
    python3 - "$OUT" "$label" "$code" "$kind" "$expected" <<'PY'
import json
import re
import sys

path, label, code, kind, expected_csv = sys.argv[1:]
expected = expected_csv.split(",")
matched_code = []
with open(path, encoding="utf-8") as output:
    for line in output:
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if record.get("reason") != "compiler-message":
            continue
        diagnostic = record["message"]
        if (diagnostic.get("code") or {}).get("code") != code:
            continue
        matched_code.append(diagnostic.get("message", ""))
        message = diagnostic.get("message", "")
        if kind == "missing":
            # Require the exact Rust identifier, not a prefix/suffix lookalike.
            terms_match = re.search(
                rf"(?<![A-Za-z0-9_]){re.escape(expected[0])}(?![A-Za-z0-9_])",
                message,
            ) is not None
        elif kind == "conversion":
            terms_match = all(
                re.search(
                    rf"(?<![A-Za-z0-9_]){re.escape(term)}(?![A-Za-z0-9_])",
                    message,
                ) is not None
                for term in expected
            )
        else:
            raise SystemExit(f"unknown diagnostic kind: {kind}")
        primary_in_source = any(
            span.get("is_primary")
            and (
                span.get("file_name", "").replace("\\", "/") == "src/main.rs"
                or span.get("file_name", "").replace("\\", "/").endswith("/src/main.rs")
            )
            and span.get("line_start", 0) > 0
            and span.get("column_end", 0) > span.get("column_start", 0)
            for span in diagnostic.get("spans", [])
        )
        if terms_match and primary_in_source:
            break
    else:
        print(
            f"{label}: expected {code} {kind} diagnostic with terms {expected!r} "
            "and a primary expression span in generated src/main.rs",
            file=sys.stderr,
        )
        print("matching-code diagnostic messages: " + repr(matched_code), file=sys.stderr)
        raise SystemExit(1)
PY
}

# custom-http alone must not expose any built-in constructor. Keep each use
# isolated so one genuine error cannot hide an accidentally exposed sibling.
expect_diagnostic anthropic_constructor '"custom-http"' E0599 missing anthropic '
use crabber_providers::HttpAdapter;
fn main() { let _ = HttpAdapter::anthropic(); }
'
expect_diagnostic openai_constructor '"custom-http"' E0599 missing openai '
use crabber_providers::HttpAdapter;
fn main() { let _ = HttpAdapter::openai(); }
'
expect_diagnostic codex_constructor '"custom-http"' E0599 missing codex '
use crabber_providers::HttpAdapter;
fn main() { let _ = HttpAdapter::codex(todo!()); }
'
expect_diagnostic opencode_go_constructor '"custom-http"' E0599 missing opencode_go '
use crabber_providers::{HttpAdapter, Protocol};
fn main() { let _ = HttpAdapter::opencode_go(Protocol::Responses); }
'

# all-providers must not expose custom-http symbols or its constructor.
expect_diagnostic auth_scheme '"all-providers"' E0432 missing AuthScheme 'use crabber_providers::AuthScheme; fn main() {}'
expect_diagnostic token_field '"all-providers"' E0432 missing ChatTokenField 'use crabber_providers::ChatTokenField; fn main() {}'
expect_diagnostic credential_source '"all-providers"' E0432 missing CredentialSource 'use crabber_providers::CredentialSource; fn main() {}'
expect_diagnostic header_hook '"all-providers"' E0432 missing RequestHeaderHook 'use crabber_providers::RequestHeaderHook; fn main() {}'
expect_diagnostic response_observer '"all-providers"' E0432 missing ResponseObserver 'use crabber_providers::ResponseObserver; fn main() {}'
expect_diagnostic error_classifier '"all-providers"' E0432 missing ErrorClassifier 'use crabber_providers::ErrorClassifier; fn main() {}'
expect_diagnostic client_config '"all-providers"' E0432 missing HttpClientConfig 'use crabber_providers::HttpClientConfig; fn main() {}'
expect_diagnostic proxy_config '"all-providers"' E0432 missing HttpProxyConfig 'use crabber_providers::HttpProxyConfig; fn main() {}'
expect_diagnostic custom_constructor '"all-providers"' E0599 missing custom '
use crabber_providers::{HttpAdapter, Protocol};
fn main() { let _ = HttpAdapter::custom("x", "https://example.test", Protocol::Responses); }
'

# The custom surface must not provide raw-client escape hatches. Each method
# and conversion is isolated and checked against its specific rustc error.
expect_diagnostic with_client '"custom-http"' E0599 missing with_client '
use crabber_providers::{HttpAdapter, Protocol};
fn main() { let _ = HttpAdapter::custom("x", "https://example.test", Protocol::Responses).with_client(reqwest::Client::new()); }
'
expect_diagnostic client_conversion '"custom-http"' E0277 conversion 'HttpClientConfig,Client,From' '
use crabber_providers::HttpClientConfig;
fn main() { let _: HttpClientConfig = reqwest::Client::new().into(); }
'
expect_diagnostic builder_conversion '"custom-http"' E0277 conversion 'HttpClientConfig,ClientBuilder,From' '
use crabber_providers::HttpClientConfig;
fn main() { let _: HttpClientConfig = reqwest::Client::builder().into(); }
'
expect_diagnostic proxy_conversion '"custom-http"' E0277 conversion 'HttpProxyConfig,Proxy,From' '
use crabber_providers::HttpProxyConfig;
fn main() { let _: HttpProxyConfig = reqwest::Proxy::all("http://localhost:8080").unwrap().into(); }
'
expect_diagnostic default_headers '"custom-http"' E0599 missing default_headers '
use crabber_providers::HttpClientConfig;
fn main() { let _ = HttpClientConfig::new().default_headers(reqwest::header::HeaderMap::new()); }
'
expect_diagnostic redirect '"custom-http"' E0599 missing redirect '
use crabber_providers::HttpClientConfig;
fn main() { let _ = HttpClientConfig::new().redirect(reqwest::redirect::Policy::none()); }
'
expect_diagnostic configure '"custom-http"' E0599 missing configure '
use crabber_providers::HttpClientConfig;
fn main() { let _ = HttpClientConfig::new().configure(|builder| builder); }
'

echo "custom-http feature probe passed"