use clap::Parser;
use server::config::{Cli, Config};

fn cli(args: &[&str]) -> Cli {
    Cli::try_parse_from(std::iter::once("miniclick").chain(args.iter().copied())).unwrap()
}

#[test]
fn defaults_and_cli_override_toml_without_resetting_other_fields() {
    let root = tempfile::tempdir().unwrap();
    let defaults = Config::load(cli(&[]), root.path()).unwrap();
    assert_eq!(defaults.node.listen_addr, "127.0.0.1:8123");
    assert_eq!(defaults.data_dir, root.path().join("./data"));
    assert_eq!(defaults.max_body_bytes, 256 * 1024 * 1024);
    std::fs::write(root.path().join("config.toml"), "listen_addr = '127.0.0.1:9000'\ndata_dir = 'saved'\nnode_id = 'from-file'\nmax_body_bytes = 100\nrole = 'Leader'\n").unwrap();
    let config = Config::load(
        cli(&[
            "--listen-addr",
            "127.0.0.1:8124",
            "--max-body-bytes",
            "1000",
        ]),
        root.path(),
    )
    .unwrap();
    assert_eq!(config.node.listen_addr, "127.0.0.1:8124");
    assert_eq!(config.max_body_bytes, 1000);
    assert_eq!(config.node.node_id, "from-file");
    assert_eq!(config.data_dir, root.path().join("saved"));
}

#[test]
fn invalid_configuration_fails_instead_of_silently_falling_back() {
    let root = tempfile::tempdir().unwrap();
    assert!(Config::load(cli(&["--config", "missing.toml"]), root.path()).is_err());
    for config in [
        "listen_addr = 'localhost'",
        "max_body_bytes = 0",
        "node_id = ''",
        "data_dir = ''",
        "role = 'Replica'",
        "unknown = 42",
        "index_granularity = 64",
        "not toml",
    ] {
        std::fs::write(root.path().join("config.toml"), config).unwrap();
        assert!(Config::load(cli(&[]), root.path()).is_err(), "{config}");
    }
}
