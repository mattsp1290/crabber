use crate::{MAX_BODY_BYTES, MAX_REDIRECTS, MAX_URL_BYTES, UrlFetchPolicy, transport::Transport};
use crabber_tools_core::ToolError;
use std::net::{IpAddr, SocketAddr};
use url::{Host, Url};
pub(crate) fn text(bytes: Vec<u8>) -> Result<String, ToolError> {
    String::from_utf8(bytes).map_err(|_| ToolError::new("binary", "resource is not UTF-8 text"))
}
pub(crate) fn too_large() -> ToolError {
    ToolError::new("too_large", "resource exceeds 1 MiB")
}
pub(crate) async fn https(
    transport: &dyn Transport,
    policy: &UrlFetchPolicy,
    mut url: Url,
) -> Result<String, ToolError> {
    for hop in 0..=MAX_REDIRECTS {
        if let Some(Host::Domain(domain)) = url.host() {
            let normalized = domain.trim_end_matches('.').to_owned();
            url.set_host(Some(&normalized))
                .map_err(|_| crate::transport::network())?;
        }
        let host = policy.check_host(&url)?;
        let port = url
            .port_or_known_default()
            .ok_or_else(crate::transport::network)?;
        let mut addresses = match url.host().unwrap() {
            Host::Ipv4(ip) => vec![SocketAddr::new(IpAddr::V4(ip), port)],
            Host::Ipv6(ip) => vec![SocketAddr::new(IpAddr::V6(ip), port)],
            Host::Domain(_) => transport.resolve(&host, port).await?,
        };
        if addresses.is_empty() || addresses.len() > 64 {
            return Err(crate::transport::network());
        }
        for address in &mut addresses {
            policy.check_ip(address.ip())?;
            address.set_port(port);
        }
        let mut response = transport.get(&url, &host, &addresses).await?;
        match response.status() {
            301 | 302 | 303 | 307 | 308 => {
                if hop == MAX_REDIRECTS {
                    return Err(ToolError::new("network", "redirect limit exceeded"));
                }
                let location = response.location()?.ok_or_else(crate::transport::network)?;
                if location.len() > MAX_URL_BYTES {
                    return Err(crate::transport::network());
                }
                url = url
                    .join(&location)
                    .map_err(|_| crate::transport::network())?;
                if url.as_str().len() > MAX_URL_BYTES {
                    return Err(crate::transport::network());
                }
                // Next iteration validates scheme, allowlist, and every new address.
            }
            404 => return Err(ToolError::new("not_found", "resource not found")),
            200..=299 => {
                if response.length().is_some_and(|n| n > MAX_BODY_BYTES as u64) {
                    return Err(too_large());
                }
                let mut bytes = Vec::new();
                while let Some(chunk) = response.chunk().await? {
                    if chunk.len() > MAX_BODY_BYTES - bytes.len() {
                        return Err(too_large());
                    }
                    bytes.extend(chunk);
                }
                return text(bytes);
            }
            _ => return Err(crate::transport::network()),
        }
    }
    unreachable!("bounded redirect loop returns on final iteration")
}
