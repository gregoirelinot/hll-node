fn main() -> Result<(), Box<dyn std::error::Error>> {
    // No system protoc required: use the vendored binary (works on Windows/Linux/macOS, x86_64/aarch64).
    if std::env::var_os("PROTOC").is_none() {
        let protoc = protoc_bin_vendored::protoc_bin_path()?;
        // SAFETY: build scripts are single-threaded at this point.
        unsafe { std::env::set_var("PROTOC", protoc) };
    }
    println!("cargo:rerun-if-changed=proto/hll.proto");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/hll.proto"], &["proto"])?;
    Ok(())
}
