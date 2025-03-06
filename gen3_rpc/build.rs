fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Hi");
    capnpc::CompilerCommand::new()
        .src_prefix("schema/")
        .file("schema/gen3rpc.capnp")
        .run()?;
    Ok(())
}
