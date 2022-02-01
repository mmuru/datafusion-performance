##Run performance test using local dataset

```shell
sh run_test.sh
```
the command will run the test using local parquet files (pems_sorted folder)

##Run performance test using remote delta table
```shell
sh run_test.sh s3://<path>
```
the command will run the test using remote delta table data (passed path)