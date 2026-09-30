#![allow(clippy::missing_panics_doc)]
use proc_macro::TokenStream;
use std::{fs, path::PathBuf};

const EXTENSIONS: &str = include_str!("../wit/crabber-extensions.wit");
const LOG: &str = include_str!("../wit/deps/crabber-host/log.wit");
const STATE: &str = include_str!("../wit/deps/crabber-host/state.wit");

#[proc_macro]
pub fn generate(input: TokenStream) -> TokenStream {
    let input = input.to_string();
    let Some(world) = input
        .strip_prefix("world : ")
        .or_else(|| input.strip_prefix("world:"))
    else {
        return "compile_error!(\"expected world: \\\"tool\\\"\");"
            .parse()
            .unwrap();
    };
    let mut parts = world.split(',');
    let world = parts.next().unwrap_or_default();
    let path = parts.next().and_then(|part| {
        part.trim()
            .strip_prefix("path: ")
            .or_else(|| part.trim().strip_prefix("path : "))
    });
    let world = world.trim().trim_matches('"');
    let dir: PathBuf = if let Some(path) = path {
        let manifest = std::env::var("CARGO_MANIFEST_DIR").expect("guest manifest directory");
        PathBuf::from(manifest).join(path.trim().trim_matches('"'))
    } else {
        let dir = std::env::temp_dir().join(format!(
            "crabber-guest-wit-{}-{}",
            env!("CARGO_PKG_VERSION"),
            std::process::id()
        ));
        let deps = dir.join("deps/crabber-host");
        if let Err(error) = fs::create_dir_all(&deps)
            .and_then(|()| fs::write(dir.join("crabber-extensions.wit"), EXTENSIONS))
            .and_then(|()| fs::write(deps.join("log.wit"), LOG))
            .and_then(|()| fs::write(deps.join("state.wit"), STATE))
        {
            return format!("compile_error!({error:?});").parse().unwrap();
        }
        dir
    };
    format!("extern crate crabber_guest as wit_bindgen; ::crabber_guest::wit_bindgen::generate!({{ world: {world:?}, path: {:?}, generate_all }});", dir.display().to_string())
        .parse()
        .unwrap()
}
