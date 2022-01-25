use chrono;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let mut ctx = ExecutionContext::new();
    ctx.register_parquet("example", "../pems_sorted").await?;

    let start_time = chrono::offset::Local::now();

    let df = ctx.sql("SELECT count(*) FROM example").await?;
    df.collect().await?;

    let end_time = chrono::offset::Local::now();
    let diff = end_time - start_time;
    println!("Start - {}", start_time);
    println!("End - {}", end_time);
    println!("Total time taken to run is {} s", diff.num_seconds());
    Ok(())
}
