echo "a new test starts"
./gen_chunk_map
./trace_analysis ../example-traces/wdev_3.csv 3
./trace_analysis ../example-traces/wdev_1.csv 3
./trace_analysis ../example-traces/rsrch_1.csv 3
./trace_analysis ../example-traces/src2_1.csv 3
