// Turns proto/rescuemesh.proto into Rust code before `common` is compiled.
// protox parses the .proto in pure Rust, so no protoc install is needed.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "../../proto/rescuemesh.proto";
    println!("cargo::rerun-if-changed={proto}");
    let descriptors = protox::compile([proto], ["../../proto"])?;
    tonic_prost_build::configure().compile_fds(descriptors)?;
    Ok(())
}
