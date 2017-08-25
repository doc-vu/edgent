#!/bin/bash

if [ $# -ne 6 ]; then
  echo 'usage:' $0 'publisher_count topic_name run_id sample_count send_interval zk_connector' 
  exit 1
fi

publisher_count=$1
topic_name=$2
run_id=$3
sample_count=$4
send_interval=$5
zk_connector=$6

mkdir -p ~/log/pub

for i in `seq 1 $publisher_count`;
do
  ( ( nohup java -cp .:edgent.jar edu.vanderbilt.edgent.clients.Publisher $topic_name $run_id $sample_count $send_interval $zk_connector  1>~/log/pub/pub_"$topic_name"_"$i".log 2>&1 ) & )
  sleep 1
done
