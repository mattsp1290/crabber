//! Real public failure observations preserve native uncertainty and exact receipts.
mod callback;
mod committed;
mod source;
use super::Error;
use crabber::session::Store;
use std::sync::Arc;

pub(crate) async fn run(store: Arc<dyn Store>) -> Result<(), Error> {
    callback::run(store.clone()).await?;
    committed::run(store.clone()).await?;
    source::run(store).await
}
