use server::{
    config::Config,
    formats::{parse_input, InputFormat},
    generator::generate_csv,
    AppState,
};
use types::{QueryOutput, Value};

#[test]
fn generator_has_reproducible_values_and_matches_the_advertised_schema() {
    let mut csv = Vec::new();
    generate_csv(&mut csv, 2, 0).unwrap();
    assert_eq!(std::str::from_utf8(&csv).unwrap(), "2026-10-01,0,-100,0,/page/0,2026-10-01 00:00:00,2026-10-01\n2026-10-02,1,-99,0.01,/page/1,2026-10-02 00:00:01,2026-10-02\n");
    let root = tempfile::tempdir().unwrap();
    let app = AppState::open(Config {
        data_dir: root.path().to_owned(),
        ..Config::default()
    })
    .unwrap();
    assert!(matches!(
        app.query.execute(server::generator::CREATE_SQL).unwrap(),
        QueryOutput::Ok
    ));
    let table = app.catalog.get_table("hits").unwrap();
    let block = parse_input(InputFormat::Csv, &csv, &table.definition().schema).unwrap();
    assert_eq!(block.columns()[2].data.get(1), Value::Int64(-99));
    assert_eq!(
        block.columns()[5].data.get(1),
        Value::parse("2026-10-02 00:00:01", types::DataType::DateTime).unwrap()
    );
    table.insert(block).unwrap();
    let QueryOutput::Rows { stream, .. } = app
        .query
        .execute("SELECT user_id FROM hits WHERE event_date = '2026-10-02'")
        .unwrap()
    else {
        panic!()
    };
    let blocks = stream.collect::<types::Result<Vec<_>>>().unwrap();
    assert_eq!(blocks.iter().map(types::Block::rows).sum::<usize>(), 1);
    assert_eq!(blocks[0].columns()[0].data.get(0), Value::UInt64(1));
    let mut overflow = Vec::new();
    assert!(generate_csv(&mut overflow, 2, u64::MAX).is_err());
    assert!(overflow.is_empty());
}
