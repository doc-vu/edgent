#!/bin/bash

if [ $# -ne 7 ]; then
  echo 'usage:' $0 'publisher_count topic_name region_id run_id sample_count send_interval zk_connector' 
  exit 1
fi

publisher_count=$1
topic_name=$2
region_id=$3
run_id=$4
sample_count=$5
send_interval=$6
zk_connector=$7

mkdir -p ~/log/pub

for i in `seq 1 $publisher_count`;
do
  ( ( nohup java -cp .:edgent.jar edu.vanderbilt.edgent.clients.Publisher $topic_name $region_id $run_id $sample_count $send_interval $zk_connector 1>~/log/pub/pub_"$topic_name"_"$i".log 2>&1 ) & )
  sleep 1
done
