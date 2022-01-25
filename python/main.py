from datetime import datetime
import datafusion


if __name__ == "__main__":
    ctx = datafusion.ExecutionContext()
    ctx.register_parquet('example', '../pems_sorted')

    start = datetime.now()

    result = ctx.sql("SELECT count(*) FROM example")
    result.collect()

    end = datetime.now()
    print("Start - {}", start)
    print("End - {}", end)
    print("Time taken {} s".format((end - start).seconds))
