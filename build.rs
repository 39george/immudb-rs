use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let root_dir = std::env::current_dir().unwrap();

    tonic_prost_build::configure()
        .out_dir(root_dir.join("src/protocol"))
        .file_descriptor_set_path(out_dir.join("types_descriptor.bin"))
        .compile_protos(
            &[
                "proto/immudb/schema.proto",
                "proto/immudb/documents.proto",
                "proto/immudb/authorization.proto",
            ],
            &["proto", "proto/grpc-gateway"],
        )
        .unwrap();
    Ok(())
}
