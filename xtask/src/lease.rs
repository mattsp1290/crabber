//! Shared cancellable lease for generated sources and build outputs.

use std::{
    fs::{self, File, OpenOptions, TryLockError},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Clone, Default)]
pub(crate) struct Lease(Arc<Mutex<Option<File>>>);
impl Lease {
    pub(crate) fn acquire(&self, path: &Path) {
        fs::create_dir_all(path.parent().expect("lease parent"))
            .expect("create persistent build output cache");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .expect("open build output lock");
        loop {
            crate::process::check_cancelled().expect("build output lock wait cancelled");
            match file.try_lock() {
                Ok(()) => break,
                Err(TryLockError::WouldBlock) => std::thread::sleep(Duration::from_millis(20)),
                Err(TryLockError::Error(error)) => {
                    panic!("cannot lock build output cache: {error}")
                }
            }
        }
        *self.0.lock().unwrap() = Some(file);
    }
    pub(crate) fn release(&self) {
        if let Some(file) = self.0.lock().unwrap().take() {
            file.unlock().expect("release build output cache lock");
        }
    }
}
