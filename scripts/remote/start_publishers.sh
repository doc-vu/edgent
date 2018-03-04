#!/bin/bash

if [ $# -ne 6 ]; then
  echo 'usage:' $0 'publisher_count topic_name sample_count send_interval payload_size zk_connector' 
  exit 1
fi

publisher_count=$1
topic_name=$2
sample_count=$3
send_interval=$4
payload_size=$5
zk_connector=$6

#extract topic number from topic string
topic_number=`echo $topic_name | cut -c 2-${#topic_name}`

mkdir -p ~/log/pub

for i in `seq 1 $publisher_count`;
do
  id=$((topic_number*10 + i))
  ( ( nohup java -cp .:edgent.jar edu.vanderbilt.edgent.endpoints.publisher.Publisher $topic_name $id $sample_count $send_interval $payload_size $zk_connector 1>~/log/pub/pub_"$topic_name"_"$id".log 2>&1 ) & )
  sleep 1
done
