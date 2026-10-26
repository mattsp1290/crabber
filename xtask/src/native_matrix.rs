use serde_json::Value;
use std::{fs, path::Path};

fn inventory() -> Value {
    serde_json::json!(
        crate::parts::native_slots()
            .map(crate::parts::Part::name)
            .collect::<Vec<_>>()
    )
}

fn admit(bytes: &[u8]) -> Result<(), String> {
    let actual: Value = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    if actual != inventory() {
        return Err("Native worker snapshot differs from canonical slots; regenerate with cargo xtask native-matrix".into());
    }
    Ok(())
}

pub(crate) fn command(args: &[String]) -> i32 {
    let result = match args {
        [] => {
            println!("{}", serde_json::to_string_pretty(&inventory()).unwrap());
            Ok(())
        }
        [flag, path] if flag == "--check" => fs::read(Path::new(path))
            .map_err(|error| error.to_string())
            .and_then(|bytes| admit(&bytes)),
        [flag] if flag == "--help" => {
            println!(
                "cargo xtask native-matrix [--check <path>]\nPrints canonical Native worker names, or rejects a stale scheduling snapshot."
            );
            Ok(())
        }
        _ => Err("usage: cargo xtask native-matrix [--check <path>]".into()),
    };
    match result {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("native-matrix: {error}");
            2
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_schedule_snapshot_and_stale_controls() {
        let bytes = include_bytes!("../../.github/native-workers.json");
        admit(bytes).expect("regenerate the committed Native worker snapshot");
        let original: Vec<String> = serde_json::from_slice(bytes).unwrap();
        for stale in [
            original[..original.len() - 1].to_vec(),
            original
                .iter()
                .cloned()
                .chain(["native-library-5".into()])
                .collect(),
            original
                .iter()
                .map(|name| name.replace("native-library-1", "native-library-0"))
                .collect(),
            original
                .iter()
                .cloned()
                .chain([original[0].clone()])
                .collect(),
        ] {
            assert!(admit(&serde_json::to_vec(&stale).unwrap()).is_err());
        }
        assert!(admit(br#"{"slots":[]}"#).is_err());
    }
}
