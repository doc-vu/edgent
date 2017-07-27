#!/bin/bash

if [ $# -ne 7 ]; then
  echo 'usage:' $0 'subscriber_count topic_name region_id run_id sample_count out_dir zk_connector' 
  exit 1
fi

subscriber_count=$1
topic_name=$2
region_id=$3
run_id=$4
sample_count=$5
out_dir=$6
zk_connector=$7

mkdir -p ~/log/sub

for i in `seq 1 $subscriber_count`;
do
  ( ( nohup java -cp .:edgent.jar edu.vanderbilt.edgent.clients.Subscriber $topic_name $region_id $run_id $sample_count $out_dir $zk_connector 1>~/log/sub/sub_"$topic_name"_"$i".log 2>&1 ) & )
  sleep 1
done
