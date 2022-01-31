##Run performance test using local dataset

```shell
cargo run
```
the command will run the test using local parquet files (pems_sorted folder)

##Run performance test using remote delta table
```shell
cargo run remote s3://<path>
```
the command will run the test using remote delta table data (passed path)