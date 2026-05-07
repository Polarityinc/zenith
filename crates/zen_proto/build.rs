fn main() {
    let proto_dir = std::path::PathBuf::from("../../proto");
    if !proto_dir.exists() {
        return;
    }
    let span_proto = proto_dir.join("zen/span.proto");
    let query_proto = proto_dir.join("zen/query.proto");
    if !span_proto.exists() || !query_proto.exists() {
        return;
    }
    let protos: Vec<&std::path::Path> = vec![span_proto.as_path(), query_proto.as_path()];
    let proto_includes: Vec<&std::path::Path> = vec![proto_dir.as_path()];
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&protos, &proto_includes)
        .unwrap();
    println!("cargo:rerun-if-changed=../../proto");
}
