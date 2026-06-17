use std::{env, fs, path::PathBuf};

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let migrations_dir = manifest_dir.join("migrations");

    println!("cargo:rerun-if-changed={}", migrations_dir.display());

    for entry in fs::read_dir(&migrations_dir).expect("read migrations directory") {
        let path = entry.expect("read migration entry").path();
        if path.extension().and_then(|extension| extension.to_str()) == Some("sql") {
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }
}
