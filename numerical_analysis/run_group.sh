#!/bin/bash
for i in {1..5}
do
echo "round:"
echo $i
./gen_chunk_map
./trace_analysis ../example-traces/prn_1.csv $1
./trace_analysis ../example-traces/prxy_1.csv $1
./trace_analysis ../example-traces/usr_1.csv $1
./trace_analysis ../example-traces/stg_1.csv $1
./trace_analysis ../example-traces/proj_4.csv $1
./trace_analysis ../example-traces/src2_1.csv $1
./trace_analysis ../example-traces/rsrch_1.csv $1
./trace_analysis ../example-traces/proj_1.csv $1
./trace_analysis ../example-traces/stg_0.csv $1
./trace_analysis ../example-traces/usr_2.csv $1
./trace_analysis ../example-traces/wdev_3.csv $1
./trace_analysis ../example-traces/wdev_1.csv $1
./trace_analysis ../example-traces/web_3.csv $1
./trace_analysis ../example-traces/web_2.csv $1
./trace_analysis ../example-traces/hm_1.csv $1
./trace_analysis ../example-traces/rsrch_2.csv $1
./trace_analysis ../example-traces/src1_0.csv $1
./trace_analysis ../example-traces/web_1.csv $1
./trace_analysis ../example-traces/src2_2.csv $1
./trace_analysis ../example-traces/prxy_0.csv $1
done
