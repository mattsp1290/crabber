use crabber_auth::{CredentialStore, FileCredentialStore, codex};
use std::{error::Error, sync::Arc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let store: Arc<dyn CredentialStore> = Arc::new(FileCredentialStore::default_crabber());
    match std::env::args().nth(1).as_deref() {
        Some("status") => println!("{:?}", codex::status(store.as_ref())?),
        Some("logout") => {
            codex::logout(store.as_ref())?;
            println!("Signed out");
        }
        None | Some("login") => {
            codex::login_browser(store, |url| println!("Open this URL in a browser:\n{url}"))
                .await?;
            println!("Signed in");
        }
        Some(_) => return Err("usage: codex-login [login|status|logout]".into()),
    }
    Ok(())
}
