//! Генерация CSV без заголовка; SQL-схема печатается отдельно в stderr.

use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(about = "Генератор демонстрационных данных MiniClick")]
struct Args {
    #[arg(long, default_value_t = 1_000_000)]
    rows: u64,
    #[arg(long, default_value_t = 0)]
    start_row: u64,
    #[arg(long)]
    output: Option<PathBuf>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    eprintln!("{};", server::generator::CREATE_SQL);
    match args.output {
        Some(path) => server::generator::generate_csv(
            std::io::BufWriter::new(std::fs::File::create(path)?),
            args.rows,
            args.start_row,
        )?,
        None => {
            server::generator::generate_csv(std::io::stdout().lock(), args.rows, args.start_row)?
        }
    }
    Ok(())
}
