//! Private roots owned by one engine; never sweep a pre-existing root.
use super::{
    config::{FIXED_ENV, PRIVATE_KEYS},
    engine::{Guard, Reason},
};
use std::path::PathBuf;

#[cfg(unix)]
fn create(path: &std::path::Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new().mode(0o700).create(path)
}
#[cfg(not(unix))]
fn create(_: &std::path::Path) -> std::io::Result<()> {
    Err(std::io::ErrorKind::Unsupported.into())
}

pub(super) fn prepare(guard: &mut Guard) -> Result<Vec<(String, String)>, Reason> {
    let mut state = guard.engine.state.lock().unwrap();
    if state.closing || guard.engine.closing.is_cancelled() {
        return Err(Reason::Closed);
    }
    if state.quarantined {
        return Err(Reason::Quarantined);
    }
    if state.root.is_none() {
        let mut root = None;
        for n in 0..16 {
            let path = guard
                .engine
                .configuration
                .temp_root
                .join(format!(".rtk-reducer-{}-{n}", std::process::id()));
            match create(&path) {
                Ok(()) => {
                    root = Some(path);
                    break;
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(_) => {
                    state.quarantined = true;
                    return Err(Reason::PrivateDirs);
                }
            }
        }
        if root.is_none() {
            state.quarantined = true;
            return Err(Reason::PrivateDirs);
        }
        state.root = root;
    }
    let dir = state
        .root
        .as_ref()
        .unwrap()
        .join(format!("inv-{}", state.seq));
    state.seq += 1;
    state.pending_dirs.insert(dir.clone());
    guard.dir = Some(dir.clone());
    create(&dir).map_err(|_| Reason::PrivateDirs)?;
    create(&dir.join("work")).map_err(|_| Reason::PrivateDirs)?;
    let mut environment = Vec::new();
    for (key, child) in PRIVATE_KEYS {
        let path = dir.join(child);
        create(&path).map_err(|_| Reason::PrivateDirs)?;
        environment.push((key.into(), path.to_str().ok_or(Reason::PrivateDirs)?.into()));
    }
    environment.extend(FIXED_ENV.map(|(k, v)| (k.into(), v.into())));
    Ok(environment)
}
pub(super) async fn remove(path: PathBuf) -> Result<(), ()> {
    tokio::task::spawn_blocking(move || match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(()),
    })
    .await
    .map_err(|_| ())?
}
