use chrono;
use datafusion::prelude::*;
use datafusion_objectstore_s3::object_store::aws::AmazonS3FileSystem;
use std::sync::Arc;
use std::env;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let mut dfp = DataFusionPerformanceTest::new();
    let args: Vec<String> = env::args().collect();
    let mut table_url: &String = &"".to_string();
    if args.len() > 1 {
        table_url = &args[1];
    }

    if table_url.starts_with("s3://") {
        dfp.run_remote(table_url).await?;
    } else {
        dfp.run_local().await?;
    }

    Ok(())
}

struct DataFusionPerformanceTest{
    ctx: ExecutionContext
}

impl DataFusionPerformanceTest {

    pub fn new() -> Self {
        Self{ ctx: ExecutionContext::new() }
    }

    async fn run_local(&mut self) -> datafusion::error::Result<()> {
        self.ctx.register_parquet("test_table","pems_sorted/").await?;
        self.run_query().await?;
        Ok(())
    }

    async fn run_remote(&mut self, source_path: &str) -> datafusion::error::Result<()> {
        let aws = Arc::new(AmazonS3FileSystem::new(None, None, None, None, None, None).await);
        self.ctx.register_object_store("s3", aws);
        self.ctx.register_parquet("test_table", source_path).await?;
        self.run_query().await?;
        Ok(())
    }

    async fn run_query(&mut self) -> datafusion::error::Result<()> {
        let df = self.ctx.sql("SELECT count(*) FROM test_table").await?;
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

}
