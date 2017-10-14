#!/bin/bash

node_name=("testbed-node8" "testbed-node9")
user="ncsgroup"
remote_cmd="make"

for name in ${node_name[*]}
do 
    scp /home/ncsgroup/zrshen/cau/* $user@$name:/home/ncsgroup/zrshen/cau
    ssh $name "cd zrshen/cau; make; exit"
done
