from datetime import datetime

import datafusion
from deltalake import DeltaTable
import sys


def do_local_test(ctx: datafusion.ExecutionContext):
    ctx.register_parquet('test_table', '../pems_sorted')
    run_query(ctx)


def do_remote_test(ctx: datafusion.ExecutionContext):
    delta_table = DeltaTable(sys.argv[2])
    pyarrow_table = delta_table.to_pyarrow_table()
    record_batch = pyarrow_table.to_batches()
    ctx.register_record_batches('test_table', [record_batch])
    run_query(ctx)


def run_query(ctx: datafusion.ExecutionContext):
    result = ctx.sql('SELECT count(*) FROM test_table')
    start = datetime.now()
    res = result.collect()
    end = datetime.now()

    result.show()
    print("Start - {}", start)
    print("End - {}", end)
    print("Time taken {} s".format((end - start).seconds))


if __name__ == "__main__":
    modes = {'local': do_local_test, 'remote': do_remote_test}
    mode = sys.argv[1]
    ctx = datafusion.ExecutionContext()
    modes[mode](ctx)
