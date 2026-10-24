use crabber_auth::{
    AccessFileCredentialStore, CredentialStore, FileCredentialStore, TokenManager,
    codex::{
        self,
        catalog::{self, CatalogRequest},
        device::DeviceLogin,
    },
};
use std::{error::Error, sync::Arc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let store: Arc<dyn CredentialStore> = Arc::new(FileCredentialStore::default_crabber());
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("status") => println!("{:?}", codex::status(store.as_ref())?),
        Some("profile") => {
            let profile = match codex::profile(store.as_ref())? {
                Some(crabber_auth::CredentialProfile::Siwc) => "siwc",
                Some(crabber_auth::CredentialProfile::CodexCli) => "codex-cli",
                None => "not logged in",
            };
            println!("{profile}");
        }
        Some("logout") => {
            codex::logout(store.as_ref())?;
            println!("Signed out");
        }
        Some("device") => {
            DeviceLogin::new(store)
                .run(
                    |p| println!("Open {} and enter {}", p.verification_url, p.user_code),
                    async { let _ = tokio::signal::ctrl_c().await; },
                )
                .await?;
            println!("Signed in (codex-cli)");
        }
        Some("catalog") if args.len() == 3 && args[1] == "--client-version" => {
            let creds = TokenManager::new(store).credentials().await?;
            let models = catalog::list_models(
                &reqwest::Client::new(),
                &creds,
                CatalogRequest {
                    client_version: &args[2],
                    base_url: None,
                    user_agent: None,
                },
            ).await?;
            for model in models {
                let efforts = model.supported_reasoning_levels.iter()
                    .map(|level| level.effort.as_str()).collect::<Vec<_>>().join(",");
                println!("{} default={} supported={efforts}", model.slug,
                    model.default_reasoning_level.as_deref().unwrap_or("none"));
            }
        }
        Some("access-status") if args.len() == 3 && args[1] == "--file" => {
            // A missing projection is a CLI input error; an existing unavailable
            // projection is a successful status lookup.
            std::fs::metadata(&args[2]).map_err(|_| crabber_auth::AuthError::Store)?;
            match AccessFileCredentialStore::new((&args[2]).into()).load()? {
                Some(credentials) => println!("available until {}", credentials.expires_unix_ms),
                None => println!("unavailable"),
            }
        }
        None | Some("login") => {
            codex::login_browser(store, |url| println!("Open this URL in a browser:\n{url}")).await?;
            println!("Signed in");
        }
        Some(_) => return Err("usage: codex-login [login|status|logout|device|profile|catalog --client-version <v>|access-status --file <path>]".into()),
    }
    Ok(())
}
