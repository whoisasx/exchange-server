use std::{env, fs, path::PathBuf};

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    print_rerun_for_migrations(&manifest_dir.join("migrations"));
    print_rerun_for_migrations(&manifest_dir.join("timeseries_migrations"));
}

fn print_rerun_for_migrations(migrations_dir: &PathBuf) {
    println!("cargo:rerun-if-changed={}", migrations_dir.display());

    for entry in fs::read_dir(&migrations_dir).expect("read migrations directory") {
        let path = entry.expect("read migration entry").path();
        if path.extension().and_then(|extension| extension.to_str()) == Some("sql") {
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }
}
