use std::fs;

fn main() {
    let epoch_out_dir = "target/epoch";
    fs::create_dir_all(epoch_out_dir).unwrap();
    tonic_build::configure()
        .build_server(true)
        .out_dir(epoch_out_dir)
        .compile(
            &["src/epoch.proto"],
            &["src"], // specify the root location to search proto dependencies
        )
        .unwrap();

    let epoch_publisher_out_dir = "target/epoch_publisher";
    fs::create_dir_all(epoch_publisher_out_dir).unwrap();
    tonic_build::configure()
        .build_server(true)
        .out_dir(epoch_publisher_out_dir)
        .compile(
            &["src/epoch_publisher.proto"],
            &["src"], // specify the root location to search proto dependencies
        )
        .unwrap();

    let range_server_out_dir = "target/rangeserver";
    fs::create_dir_all(range_server_out_dir).unwrap();
    tonic_build::configure()
        .build_server(true)
        .out_dir(range_server_out_dir)
        .compile(
            &["src/rangeserver.proto"],
            &["src"], // specify the root location to search proto dependencies
        )
        .unwrap();

    let warden_out_dir = "target/warden";
    fs::create_dir_all(warden_out_dir).unwrap();
    tonic_build::configure()
        .build_server(true)
        .out_dir(warden_out_dir)
        .compile(
            &["src/warden.proto"],
            &["src"], // specify the root location to search proto dependencies
        )
        .unwrap();

    let universe_out_dir = "target/universe";
    fs::create_dir_all(universe_out_dir).unwrap();
    tonic_build::configure()
        .build_server(true)
        .out_dir(universe_out_dir)
        .compile(
            &["src/universe.proto"],
            &["src"], // specify the root location to search proto dependencies
        )
        .unwrap();

    let frontend_out_dir = "target/frontend";
    fs::create_dir_all(frontend_out_dir).unwrap();
    tonic_build::configure()
        .build_server(true)
        .out_dir(frontend_out_dir)
        .compile(
            &["src/frontend.proto"],
            &["src"], // specify the root location to search proto dependencies
        )
        .unwrap();
}
