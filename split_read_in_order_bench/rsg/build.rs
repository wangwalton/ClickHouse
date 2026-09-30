fn main() {
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());
    tonic_build::configure().build_server(false).compile_protos(&["proto/clickhouse_grpc.proto"], &["proto"]).unwrap();
}
