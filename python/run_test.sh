if [ -z "$1" ]
  then
  tar xvzf ../pems_parquet.zip
fi

python main.py $1

if [ -z "$1" ]
  then
  rm -r pems_sorted
fi
