use clap::Parser;
use server::config::{Cli, Config};
use server::AppState;
use types::DbError;

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
    assert!(defaults.sharding.is_empty());
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
        "cluster_id = ''",
        "data_dir = ''",
        "role = 'Replica'",
        "role = 'Replica'\nleader_addr = '127.0.0.1:9001'",
        "role = 'Replica'\nleader_addr = 'localhost'\nleader_id = 'leader-1'",
        "role = 'Replica'\nleader_addr = '127.0.0.1:9001'\nleader_id = 'leader-1'\nsync_interval_ms = 0",
        "role = 'unknown'",
        "unknown = 42",
        "index_granularity = 64",
        "not toml",
    ] {
        std::fs::write(root.path().join("config.toml"), config).unwrap();
        assert!(Config::load(cli(&[]), root.path()).is_err(), "{config}");
    }
}

#[test]
fn replica_role_parses_identity_and_sync_interval() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path().join("config.toml"),
        "cluster_id = 'cluster-1'\nrole = 'Replica'\nleader_addr = '127.0.0.1:9001'\nleader_id = 'leader-1'\nsync_interval_ms = 250\n",
    )
    .unwrap();
    let config = Config::load(cli(&[]), root.path()).unwrap();
    assert_eq!(config.node.cluster_id, "cluster-1");
    assert_eq!(
        config.node.role,
        replication::Role::Replica {
            leader_addr: "127.0.0.1:9001".into(),
            leader_id: "leader-1".into(),
        }
    );
    assert_eq!(config.node.sync_interval_ms, 250);
}

#[test]
fn replica_accepts_dns_name_without_resolving_it_during_config_loading() {
    let root = tempfile::tempdir().unwrap();
    for address in ["leader:8123", "127.0.0.1:8123", "[::1]:8123"] {
        let config = Config::load(
            cli(&[
                "--role",
                "Replica",
                "--leader-addr",
                address,
                "--leader-id",
                "leader-1",
            ]),
            root.path(),
        )
        .unwrap();
        assert_eq!(
            config.node.role,
            replication::Role::Replica {
                leader_addr: address.into(),
                leader_id: "leader-1".into(),
            }
        );
    }
}

#[test]
fn replica_rejects_leader_addresses_without_explicit_valid_host_and_port() {
    let root = tempfile::tempdir().unwrap();
    for address in [
        "leader",
        "leader:",
        ":8123",
        "leader:65536",
        "leader:abc",
        "http://leader:8123",
        "user@leader:8123",
        "leader:8123/path",
        "leader:8123?x=1",
        "leader:8123#fragment",
    ] {
        assert!(
            Config::load(
                cli(&[
                    "--role",
                    "Replica",
                    "--leader-addr",
                    address,
                    "--leader-id",
                    "leader-1"
                ]),
                root.path(),
            )
            .is_err(),
            "{address}"
        );
    }
}

#[test]
fn cluster_config_accepts_three_members_without_resolving_dns() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("config.toml"), "[cluster]\nid = 2\nmembers = { '1' = 'node1:8123', '2' = 'node2:8123', '3' = 'node3:8123' }\n").unwrap();
    Config::load(cli(&[]), root.path()).unwrap();
}

#[test]
fn cluster_config_rejects_ambiguous_members_and_static_replica_role() {
    let root = tempfile::tempdir().unwrap();
    for configuration in [
        "[cluster]\nid = 4\nmembers = { '1' = 'node1:8123', '2' = 'node2:8123', '3' = 'node3:8123' }",
        "[cluster]\nid = 1\nmembers = { '1' = 'node1:8123', '2' = 'node2:8123' }",
        "[cluster]\nid = 1\nmembers = { '1' = 'node1:8123', '2' = 'node1:8123', '3' = 'node3:8123' }",
        "[cluster]\nid = 1\nmembers = { '1' = 'node1', '2' = 'node2:8123', '3' = 'node3:8123' }",
        "role = 'Replica'\nleader_addr = 'node1:8123'\nleader_id = 'node1'\n[cluster]\nid = 2\nmembers = { '1' = 'node1:8123', '2' = 'node2:8123', '3' = 'node3:8123' }",
    ] {
        std::fs::write(root.path().join("config.toml"), configuration).unwrap();
        assert!(Config::load(cli(&[]), root.path()).is_err(), "{configuration}");
    }
}

const SHARDING_CONFIG: &str = r#"
[sharding.Hits]
local_table = "hits_local"
key = "user_id"
shards = { "10" = "shard10:8123", "7" = "[::1]:8123", "2" = "shard2:8123" }
"#;

#[test]
fn sharding_routes_full_width_keys_by_numeric_shard_id() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("config.toml"), SHARDING_CONFIG).unwrap();
    let config = Config::load(cli(&[]), root.path()).unwrap();
    let table = &config.sharding["Hits"];
    assert!(!config.sharding.contains_key("hits"));
    assert_eq!(table.local_table, "hits_local");
    assert_eq!(table.key, "user_id");
    assert_eq!(table.shard_for_key(0), Some((2, "shard2:8123")));
    assert_eq!(table.shard_for_key(1 << 32), Some((7, "[::1]:8123")));
    assert_eq!(table.shard_for_key(5), Some((10, "shard10:8123")));
    assert_eq!(table.shard_for_key(u64::MAX), Some((2, "shard2:8123")));
}

#[test]
fn saved_coordinator_topology_rejects_changed_ids_keys_and_removed_routes() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("config.toml");
    std::fs::write(&path, SHARDING_CONFIG).unwrap();
    let config = Config::load(cli(&[]), root.path()).unwrap();
    drop(AppState::open(config.clone()).unwrap());

    std::fs::write(
        &path,
        "[sharding.Hits]\nlocal_table = 'hits_local'\nkey = 'user_id'\nshards = { '7' = '[::1]:8123', '2' = 'shard2:8123', '10' = 'shard10:8123' }",
    )
    .unwrap();
    drop(AppState::open(Config::load(cli(&[]), root.path()).unwrap()).unwrap());
    for changed in [
        SHARDING_CONFIG.replace(r#""7" ="#, r#""8" ="#),
        SHARDING_CONFIG.replace("user_id", "new_key"),
        SHARDING_CONFIG.replace("hits_local", "other_table"),
        String::new(),
    ] {
        std::fs::write(&path, &changed).unwrap();
        assert!(
            matches!(
                AppState::open(Config::load(cli(&[]), root.path()).unwrap()),
                Err(DbError::Syntax(message)) if message.contains("sharding topology differs")
            ),
            "{changed}"
        );
    }
    drop(AppState::open(config).unwrap());
}

#[test]
fn sharding_rejects_invalid_tables_and_ambiguous_destinations() {
    let root = tempfile::tempdir().unwrap();
    for (original, replacement) in [
        ("sharding.Hits", "sharding.\"\""),
        ("hits_local", ""),
        ("user_id", ""),
        (
            r#"{ "10" = "shard10:8123", "7" = "[::1]:8123", "2" = "shard2:8123" }"#,
            "{}",
        ),
        (r#""7" ="#, r#""0" ="#),
        (r#""7" ="#, r#""07" ="#),
        (r#""7" ="#, r#""10" ="#),
        ("shard10:8123", "shard2:8123"),
        ("shard10:8123", "shard10"),
    ] {
        let config = SHARDING_CONFIG.replace(original, replacement);
        std::fs::write(root.path().join("config.toml"), &config).unwrap();
        assert!(
            matches!(
                Config::load(cli(&[]), root.path()),
                Err(types::DbError::Syntax(_))
            ),
            "{config}"
        );
    }
}

#[test]
fn coordinator_schema_requires_unique_columns_and_a_uint64_shard_key() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("config.toml");
    let schema = "\n[sharding.Hits.schema]\ncolumns = [{ name = 'user_id', data_type = 'UInt64' }, { name = 'label', data_type = 'String' }]\n";
    std::fs::write(&path, format!("{SHARDING_CONFIG}{schema}")).unwrap();
    let config = Config::load(cli(&[]), root.path()).unwrap();
    assert_eq!(
        config.sharding["Hits"].schema.as_ref().unwrap().names(),
        ["user_id", "label"]
    );
    for invalid in [
        schema.replace("'UInt64'", "'Int64'"),
        schema.replace("'user_id'", "'other_id'"),
        schema.replace("'label'", "'user_id'"),
    ] {
        std::fs::write(&path, format!("{SHARDING_CONFIG}{invalid}")).unwrap();
        assert!(matches!(
            Config::load(cli(&[]), root.path()),
            Err(types::DbError::Syntax(_))
        ));
    }
}

#[test]
fn raft_shards_require_distinct_cluster_ids_and_member_addresses() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("config.toml");
    let config = format!("{SHARDING_CONFIG}\n[sharding.Hits.raft_shards.'2']\ncluster_id = 'even'\nmembers = ['shard2:8123', 'even2:8123', 'even3:8123']\n[sharding.Hits.raft_shards.'10']\ncluster_id = 'odd'\nmembers = ['shard10:8123', 'odd2:8123', 'odd3:8123']");
    std::fs::write(&path, &config).unwrap();
    let loaded = Config::load(cli(&[]), root.path()).unwrap();
    assert_eq!(loaded.sharding["Hits"].raft_shards[&10].cluster_id, "odd");
    for (original, replacement) in [
        ("cluster_id = 'odd'", "cluster_id = 'even'"),
        ("odd2:8123", "even2:8123"),
        ("odd2:8123", "[::1]:8123"),
    ] {
        std::fs::write(&path, config.replace(original, replacement)).unwrap();
        assert!(
            matches!(
                Config::load(cli(&[]), root.path()),
                Err(types::DbError::Syntax(_))
            ),
            "{replacement}"
        );
    }
}

#[test]
fn sharding_coordinator_requires_a_standalone_leader() {
    let root = tempfile::tempdir().unwrap();
    for configuration in [
        format!("role = 'Replica'\nleader_addr = 'node1:8123'\nleader_id = 'node1'\n{SHARDING_CONFIG}"),
        format!("{SHARDING_CONFIG}\n[cluster]\nid = 1\nmembers = {{ '1' = 'node1:8123', '2' = 'node2:8123', '3' = 'node3:8123' }}"),
    ] {
        std::fs::write(root.path().join("config.toml"), configuration).unwrap();
        assert!(matches!(
            Config::load(cli(&[]), root.path()),
            Err(types::DbError::Unsupported(_))
        ));
    }
}

#[test]
fn local_shard_configuration_requires_a_valid_id_set() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("config.toml");
    let header = "[local_shards.hits_local]\nkey = 'id'\nshard_id = 2\n";
    std::fs::write(&path, format!("{header}shard_ids = [10, 2]")).unwrap();
    let config = Config::load(cli(&[]), root.path()).unwrap();
    assert_eq!(config.local_shards["hits_local"].shard_ids, [10, 2]);

    for ids in ["[]", "[10]", "[2, 2]", "[0, 2]"] {
        std::fs::write(&path, format!("{header}shard_ids = {ids}")).unwrap();
        assert!(
            matches!(
                Config::load(cli(&[]), root.path()),
                Err(types::DbError::Syntax(_))
            ),
            "{ids}"
        );
    }
}
