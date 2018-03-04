#!/bin/bash

if [ $# -ne 5 ]; then
  echo 'usage:' $0 'subscriber_count topic_name run_id sample_count out_dir' 
  exit 1
fi

subscriber_count=$1
topic_name=$2
run_id=$3
sample_count=$4
out_dir=$5

#extract topic number from topic string
topic_number=`echo $topic_name | cut -c 2-${#topic_name}`

mkdir -p ~/log/sub

for i in `seq 1 $subscriber_count`;
do
  id=$((topic_number*10 + i))
  ( ( nohup java -cp .:edgent.jar edu.vanderbilt.edgent.endpoints.subscriber.Subscriber $topic_name $id $sample_count $run_id $out_dir  1>~/log/sub/sub_"$topic_name"_"$id".log 2>&1 ) & )
  sleep 1
done
