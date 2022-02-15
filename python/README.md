##Run performance test using local dataset
 
  - python -m venv venv
  - source venv/bin/activate
  - pip install -U pip
  - pip install -r requirements.txt

```shell
sh run_test.sh
```
the command will run the test using local parquet files (pems_sorted folder)

##Run performance test using remote delta table
```shell
sh run_test.sh s3://<delta-table-path>
```
the command will run the test using remote delta table data