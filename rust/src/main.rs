use chrono;
use datafusion::prelude::*;
use datafusion_objectstore_s3::object_store::aws::AmazonS3FileSystem;
use std::sync::Arc;
use std::env;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let args: Vec<String> = env::args().collect();
    let ctx = ExecutionContext::new();

    let mode = &args[1];
    if mode == "remote" {
        do_remote_test(ctx, &args[2]).await?;
    } else {
        do_local_test(ctx).await?;
    }

    Ok(())
}

async fn do_local_test(mut ctx: ExecutionContext) -> datafusion::error::Result<()> {
    ctx.register_parquet("test_table","../pems_sorted/").await?;
    run_query(ctx).await?;
    Ok(())
}

async fn do_remote_test(mut ctx: ExecutionContext, source_path: &str) -> datafusion::error::Result<()> {
    let aws = Arc::new(AmazonS3FileSystem::new(None, None, None, None, None, None).await);
    ctx.register_object_store("s3", aws);
    ctx.register_parquet("test_table", source_path).await?;
    run_query(ctx).await?;
    Ok(())
}

async fn run_query(mut ctx: ExecutionContext) -> datafusion::error::Result<()> {
    let df = ctx.sql("SELECT count(*) FROM test_table").await?;
    let start_time = chrono::offset::Local::now();
    df.collect().await?;

    let end_time = chrono::offset::Local::now();
    let diff = end_time - start_time;
    df.show().await?;
    println!("Start - {}", start_time);
    println!("End - {}", end_time);
    println!("Total time taken to run is {} s", diff.num_seconds());

    Ok(())
}
