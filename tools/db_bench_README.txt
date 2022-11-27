## Creating a new DB and filling it with random 1 billion keys

./db_bench --compression_type=None -db=/data/ -num=1000000000 -value_size=64 -key_size=16 --delayed_write_rate=536870912 -report_interval_seconds=1 -max_write_buffer_number=4 -num_column_families=1 -histogram -max_background_compactions=8 -max_background_flushes=4 -bloom_bits=10 --report_file=fillrandom.csv --disable_wal=true --benchmarks=fillrandom


## Running random reads and write on the above DB

./db_bench --compression_type=None -db=/data/ -num=1000000000 -value_size=64 -key_size=16 --delayed_write_rate=536870912 -report_interval_seconds=1 -max_write_buffer_number=4 -num_column_families=1 -histogram -max_background_compactions=8 -max_background_flushes=4 -bloom_bits=10 -duration=900 --use_existing_db -threads=50 -readwritepercent=50 -report_file=readrandomwriterandom_50.csv --benchmarks=readrandomwriterandom -write_buffer_size=268435456 

Note: The default memtable in this db_bench tool is Speedb sorted hash memtable. 
