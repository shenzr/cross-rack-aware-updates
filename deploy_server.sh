#!/bin/bash

node_name=("13.125.237.65" "13.125.237.16" "13.209.3.43" "13.125.234.148" "13.125.252.243" 
		   "54.169.215.182" "13.250.30.81" "54.254.200.139" "54.169.253.138" "13.229.215.19" 
		   "13.211.92.92" "54.206.12.247" "13.210.59.174" "54.252.161.166" "52.63.192.168" 
		   "54.250.246.253" "13.114.248.187" "13.230.239.222" "13.231.84.103" "54.249.114.24")

user="ubuntu"

for name in ${node_name[*]}
do 
    scp $1 $user@$name:/home/ubuntu
done
