use crabber::ExtensionError;
use crabber_tools_core::ToolError;
use serde::Serialize;
use std::net::IpAddr;
use url::{Host, Url};
/// A normalized DNS name or literal IP host allowed by the host application.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HostPattern {
    /// This exact host only.
    Exact(String),
    /// Proper subdomains of this DNS suffix; excludes the suffix itself.
    Subdomains(String),
}
fn normalize(value: &str) -> Option<String> {
    if value.is_empty() || value.len() > 253 || value.contains(['/', ':', '@', '*', '\0']) {
        return None;
    }
    Some(Host::parse(value.trim_end_matches('.')).ok()?.to_string())
}
impl HostPattern {
    /// Normalize an exact DNS name or IPv4 literal. IPv6 literals use `Exact` with brackets.
    pub fn exact(value: &str) -> Result<Self, ExtensionError> {
        let normalized = if value.starts_with('[') {
            Host::parse(value).ok().map(|h| h.to_string())
        } else {
            normalize(value)
        };
        normalized
            .map(Self::Exact)
            .ok_or_else(|| ExtensionError::Plan("URL host pattern".into()))
    }
    /// Normalize a proper DNS subdomain suffix.
    pub fn subdomains(value: &str) -> Result<Self, ExtensionError> {
        let value =
            normalize(value).ok_or_else(|| ExtensionError::Plan("URL host pattern".into()))?;
        if !matches!(Host::parse(&value), Ok(Host::Domain(_))) {
            return Err(ExtensionError::Plan("URL host pattern".into()));
        }
        Ok(Self::Subdomains(value))
    }
    fn matches(&self, host: &str) -> bool {
        match self {
            Self::Exact(value) => value == host,
            Self::Subdomains(value) => host
                .strip_suffix(value)
                .is_some_and(|p| p.ends_with('.') && p.len() > 1),
        }
    }
}
/// Explicit network policy. An allowlist never overrides address restrictions.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct UrlFetchPolicy {
    /// Empty allows any host that passes address restrictions.
    pub allow: Vec<HostPattern>,
    /// Reject non-public IPv4 and IPv6 destinations at each connection.
    pub deny_private_ranges: bool,
}
impl Default for UrlFetchPolicy {
    fn default() -> Self {
        Self {
            allow: Vec::new(),
            deny_private_ranges: true,
        }
    }
}
impl UrlFetchPolicy {
    /// Validate normalized policy values before mounting.
    pub fn validate(&self) -> Result<(), ExtensionError> {
        for pattern in &self.allow {
            let rebuilt = match pattern {
                HostPattern::Exact(v) => HostPattern::exact(v)?,
                HostPattern::Subdomains(v) => HostPattern::subdomains(v)?,
            };
            if &rebuilt != pattern {
                return Err(ExtensionError::Plan(
                    "URL host pattern must be normalized".into(),
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn check_host(&self, url: &Url) -> Result<String, ToolError> {
        if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
            return Err(ToolError::new(
                "validation",
                "use https without URL user information",
            ));
        }
        let host = url
            .host()
            .ok_or_else(|| ToolError::new("validation", "URL host is required"))?
            .to_string();
        let host = host.trim_end_matches('.').to_owned();
        if !self.allow.is_empty() && !self.allow.iter().any(|p| p.matches(&host)) {
            return Err(ToolError::new("validation", "URL host is not allowed"));
        }
        Ok(host)
    }
    pub(crate) fn check_ip(&self, ip: IpAddr) -> Result<(), ToolError> {
        if self.deny_private_ranges && !is_public(ip) {
            return Err(ToolError::new(
                "validation",
                "URL destination is not public",
            ));
        }
        Ok(())
    }
}
fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !(a == 0
                || a == 10
                || a == 127
                || a >= 224
                || (a == 100 && (64..=127).contains(&b))
                || (a == 169 && b == 254)
                || (a == 172 && (16..=31).contains(&b))
                || (a == 192 && (b == 168 || (b == 0 && (c == 0 || c == 2))))
                || (a == 198 && (b == 18 || b == 19 || (b == 51 && c == 100)))
                || (a == 203 && b == 0 && c == 113)
                || (a == 192 && b == 88 && c == 99))
        }
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return is_public(IpAddr::V4(mapped));
            }
            let s = ip.segments();
            // Only ordinary global unicast; exclude documentation, special-purpose
            // and transition prefixes that can embed private IPv4 destinations.
            s[0] & 0xe000 == 0x2000
                && s[0] != 0x2002
                && !(s[0] == 0x3fff && s[1] & 0xf000 == 0)
                && !(s[0] == 0x2001 && (s[1] < 0x200 || s[1] == 0xdb8))
        }
    }
}
