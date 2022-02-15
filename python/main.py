from datetime import datetime

import datafusion
from deltalake import DeltaTable
import sys


class DataFusionPerformanceTest:
    def __init__(self):
        self.ctx = datafusion.ExecutionContext()

    def run_local(self):
        self.ctx.register_parquet("test_table", "pems_sorted")
        self.perform_query()

    def run_remote(self, delta_table_url, partition_filter_list=None):
        delta_table = DeltaTable(delta_table_url)

        if partition_filter_list:
            pyarrow_table = delta_table.to_pyarrow_table(partition_filter_list)
        else:
            pyarrow_table = delta_table.to_pyarrow_table()

        record_batch = pyarrow_table.to_batches()
        self.ctx.register_record_batches("test_table", [record_batch])
        self.perform_query()

    def perform_query(self):
        result = self.ctx.sql("SELECT count(*) FROM test_table")
        start = datetime.now()
        res = result.collect()
        end = datetime.now()

        result.show()
        print("Start - {}", start)
        print("End - {}", end)
        print("Time taken {} s".format((end - start).seconds))


if __name__ == "__main__":

    performance_test = DataFusionPerformanceTest()
    table_url = None
    if len(sys.argv) > 1:
        table_url = sys.argv[1]

    if table_url and table_url.startswith("s3://"):
        year_tuple = ("file_year", "=", "2019")
        month_tuple = ("file_month", "=", "5")
        partition_filter_list = [year_tuple, month_tuple]
        performance_test.run_remote(table_url, partition_filter_list)
    else:
        performance_test.run_local()
