fn main() {
    println!("cargo:rerun-if-changed=assets/icon.ico");
    println!("cargo:rerun-if-changed=assets/icon.png");
    println!("cargo:rerun-if-changed=assets/dice-icon.rc");

    // Only embed the Windows app icon when building the GUI.
    let gui = std::env::var_os("CARGO_FEATURE_GUI").is_some();
    if gui {
        #[cfg(target_os = "windows")]
        {
            let _ = embed_resource::compile("assets/dice-icon.rc", embed_resource::NONE);
        }
    }
}