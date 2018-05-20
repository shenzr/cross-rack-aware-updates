#!/bin/bash

node_name=("13.229.209.179")

user="ubuntu"

for name in ${node_name[*]}
do 
    scp $1 $user@$name:/home/ubuntu
done
