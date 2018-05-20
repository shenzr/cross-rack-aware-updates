#!/bin/bash

node_name=("testbed-node5","testbed-node6","testbed-node7","testbed-node8" "testbed-node9","testbed-node10")
user="ncsgroup"
remote_cmd="make"

for name in ${node_name[*]}
do 
    scp -r /home/ncsgroup/zrshen/cau/* $user@$name:/home/ncsgroup/zrshen/cau
done
