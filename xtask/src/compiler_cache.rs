//! Direct checks replace themselves with the same measured Python owner.
use std::{env, fs, io, path::Path, process::Command};

pub(crate) fn configure_command(command: &mut Command) {
    if env::var("CRABBER_COMPILER_CACHE").as_deref() != Ok("local") {
        let names = env::vars_os()
            .map(|(name, _)| name)
            .chain(command.get_envs().map(|(name, _)| name.to_os_string()))
            .filter(|name| name.as_encoded_bytes().starts_with(b"SCCACHE_"))
            .collect::<Vec<_>>();
        for name in names {
            command.env_remove(name);
        }
        command
            .env_remove("RUSTC_WRAPPER")
            .env_remove("RUSTC_WORKSPACE_WRAPPER");
    }
}

pub(crate) fn enter(args: &[String]) {
    if let Err(error) = enter_checked(args) {
        eprintln!("{error}; recovery: CRABBER_COMPILER_CACHE=off cargo xtask check");
        std::process::exit(2);
    }
}

fn enter_checked(args: &[String]) -> Result<(), String> {
    if args.first().map(String::as_str) != Some("check") || args.iter().any(|arg| arg == "--list") {
        return Ok(());
    }
    let mode = env::var("CRABBER_COMPILER_CACHE").unwrap_or_else(|_| "off".into());
    match mode.as_str() {
        "off" => {
            if env::var_os("CRABBER_COMPILER_CACHE_SESSION").is_some() {
                return Err("off mode cannot join a compiler-cache session".into());
            }
            Ok(())
        }
        "local" => {
            if env::var("CARGO_INCREMENTAL").as_deref() != Ok("0") {
                return Err(
                    "local compiler cache requires CARGO_INCREMENTAL=0 before outer Cargo".into(),
                );
            }
            if let Some(descriptor) = env::var_os("CRABBER_COMPILER_CACHE_SESSION") {
                validate(Path::new(&descriptor))
            } else {
                let mut command = Command::new("python3");
                command
                    .args(["-I", "-S"])
                    .arg(crate::workspace().join(".github/scripts/compiler-cache.py"))
                    .args(["run-direct", "--"])
                    .arg(env::current_exe().map_err(|error| error.to_string())?)
                    .args(args)
                    .current_dir(crate::workspace());
                #[cfg(unix)]
                {
                    use std::os::unix::process::CommandExt;
                    Err(command.exec().to_string())
                }
                #[cfg(not(unix))]
                Err("compiler-cache supervision requires Linux".into())
            }
        }
        _ => Err("CRABBER_COMPILER_CACHE must be off or local".into()),
    }
}

fn validate(path: &Path) -> Result<(), String> {
    use sha2::{Digest, Sha256};
    let read = || -> io::Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let metadata = fs::symlink_metadata(path)?;
            if !metadata.is_file() || metadata.mode() & 0o777 != 0o600 {
                return Err(io::Error::other("invalid compiler session permissions"));
            }
        }
        let data: serde_json::Value = serde_json::from_slice(&fs::read(path)?)?;
        let fields = [
            "schema",
            "owner_pid",
            "owner_start",
            "server_pid",
            "server_start",
            "executable",
            "executable_sha256",
            "wrapper",
            "wrapper_sha256",
            "socket",
            "store",
            "config",
            "namespace",
        ];
        let object = data
            .as_object()
            .ok_or_else(|| io::Error::other("invalid compiler session"))?;
        if object.len() != fields.len()
            || fields.iter().any(|field| !object.contains_key(*field))
            || data["schema"] != 1
        {
            return Err(io::Error::other("invalid compiler session fields"));
        }
        for kind in ["owner", "server"] {
            let pid = data[format!("{kind}_pid")]
                .as_u64()
                .ok_or_else(|| io::Error::other("invalid owner PID"))?;
            let expected = data[format!("{kind}_start")]
                .as_u64()
                .ok_or_else(|| io::Error::other("invalid owner lease"))?;
            let process = fs::read_to_string(format!("/proc/{pid}/stat"))?;
            let start = process
                .rsplit_once(')')
                .and_then(|(_, rest)| rest.split_whitespace().nth(19))
                .and_then(|value| value.parse::<u64>().ok());
            if start != Some(expected) {
                return Err(io::Error::other("compiler cache owner lease expired"));
            }
        }
        let string = |name: &str| {
            data[name]
                .as_str()
                .ok_or_else(|| io::Error::other("invalid compiler session path"))
        };
        let executable = string("executable")?;
        let wrapper = string("wrapper")?;
        if env::var("RUSTC_WRAPPER").as_deref() != Ok(wrapper)
            || format!("{:x}", Sha256::digest(fs::read(wrapper)?)) != string("wrapper_sha256")?
            || format!("{:x}", Sha256::digest(fs::read(executable)?))
                != string("executable_sha256")?
        {
            return Err(io::Error::other("compiler session wrapper changed"));
        }
        for (field, variable) in [
            ("socket", "SCCACHE_SERVER_UDS"),
            ("store", "SCCACHE_DIR"),
            ("config", "SCCACHE_CONF"),
        ] {
            if env::var(variable).as_deref() != Ok(string(field)?) {
                return Err(io::Error::other("compiler session configuration changed"));
            }
        }
        if env::var("SCCACHE_CLIENT_SIDE").as_deref() != Ok("1")
            || env::var("SCCACHE_NO_DAEMON").as_deref() != Ok("1")
            || env::var_os("SCCACHE_START_SERVER").is_some()
        {
            return Err(io::Error::other(
                "compiler session must use supervised client-side compilation",
            ));
        }
        Ok(())
    };
    read().map_err(|error| format!("compiler cache descriptor rejected: {error}"))
}

#[cfg(test)]
mod tests {
    #[test]
    fn rejects_unissued_descriptor() {
        let path =
            std::env::temp_dir().join(format!("crabber-invalid-session-{}", std::process::id()));
        std::fs::write(&path, b"{}").unwrap();
        assert!(super::validate(&path).is_err());
        std::fs::remove_file(path).unwrap();
    }
}
