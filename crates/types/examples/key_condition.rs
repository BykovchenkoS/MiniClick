use types::{may_match, BinaryOp, Expr, KeyRange, Value};

fn main() {
    let predicate = Expr::binary(
        BinaryOp::GtEq,
        Expr::col("id"),
        Expr::lit(Value::UInt64(100)),
    );
    let key = vec!["id".to_string()];
    println!("WHERE id >= 100");
    for (min, max) in [(1, 30), (31, 70), (71, 150), (151, 300)] {
        let range = KeyRange {
            min: vec![Value::UInt64(min)],
            max: vec![Value::UInt64(max)],
        };
        let result = if may_match(&predicate, &key, &range) {
            "READ: possible match"
        } else {
            "SKIP: no match"
        };
        println!("[{min}, {max}] -> {result}");
    }
}
