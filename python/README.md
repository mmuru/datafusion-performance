##Run performance test using local dataset

```shell
python main.py local
```
the command will run the test using local parquet files (pems_sorted folder)

##Run performance test using remote delta table
```shell
python main.py remote s3://<path>
```
the command will run the test using remote delta table data (passed path)