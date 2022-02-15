if [ -z "$1" ]
  then
  tar xzf ../pems_parquet.zip
fi

cargo run $1

if [ -z "$1" ]
  then
  rm -r pems_sorted
fi

