use crate::TIMEOUT;
use async_trait::async_trait;
use crabber_tools_core::ToolError;
use std::net::SocketAddr;
use url::Url;
pub(crate) fn network() -> ToolError {
    ToolError::new("network", "URL request failed")
}
#[async_trait]
pub(crate) trait Body: Send {
    fn status(&self) -> u16;
    fn location(&self) -> Result<Option<String>, ToolError>;
    fn length(&self) -> Option<u64>;
    async fn chunk(&mut self) -> Result<Option<Vec<u8>>, ToolError>;
}
#[async_trait]
pub(crate) trait Transport: Send + Sync {
    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, ToolError>;
    async fn get(
        &self,
        url: &Url,
        host: &str,
        addresses: &[SocketAddr],
    ) -> Result<Box<dyn Body>, ToolError>;
}
pub(crate) struct Https;
#[async_trait]
impl Transport for Https {
    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, ToolError> {
        let resolved = tokio::net::lookup_host((host, port))
            .await
            .map_err(|_| network())?;
        let mut addresses = Vec::new();
        for address in resolved {
            if addresses.len() == 64 {
                return Err(network());
            }
            addresses.push(address);
        }
        Ok(addresses)
    }
    async fn get(
        &self,
        url: &Url,
        host: &str,
        addresses: &[SocketAddr],
    ) -> Result<Box<dyn Body>, ToolError> {
        // A new client per hop prevents pooled connections or cached DNS from
        // bypassing this hop's checked, pinned addresses. Preserve URL host/SNI.
        let client = reqwest::Client::builder()
            .use_rustls_tls()
            .https_only(true)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .referer(false)
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .timeout(TIMEOUT)
            .resolve_to_addrs(host, addresses)
            .build()
            .map_err(|_| network())?;
        let response = client
            .get(url.clone())
            .send()
            .await
            .map_err(|_| network())?;
        Ok(Box::new(response))
    }
}
#[async_trait]
impl Body for reqwest::Response {
    fn status(&self) -> u16 {
        self.status().as_u16()
    }
    fn location(&self) -> Result<Option<String>, ToolError> {
        self.headers()
            .get(reqwest::header::LOCATION)
            .map(|v| {
                if v.as_bytes().len() > crate::MAX_URL_BYTES {
                    return Err(network());
                }
                v.to_str().map(str::to_owned).map_err(|_| network())
            })
            .transpose()
    }
    fn length(&self) -> Option<u64> {
        self.content_length()
    }
    async fn chunk(&mut self) -> Result<Option<Vec<u8>>, ToolError> {
        reqwest::Response::chunk(self)
            .await
            .map(|c| c.map(|c| c.to_vec()))
            .map_err(|_| network())
    }
}
