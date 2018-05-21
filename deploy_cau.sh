#!/bin/bash

node_name=("testbed-node52" "testbed-node51" "testbed-node42" "testbed-node43" "testbed-node44" "testbed-node45" "testbed-node46" "testbed-node47"  "testbed-node49" "testbed-node50")
user="ncsgroup"
remote_cmd="make"

for name in ${node_name[*]}
do 
    scp /home/ncsgroup/zrshen/cross-rack-aware-update-github/* $user@$name:/home/ncsgroup/zrshen/cross-rack-aware-update-github
done
