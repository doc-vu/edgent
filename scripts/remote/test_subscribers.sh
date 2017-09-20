#!/bin/bash

if [ $# -ne 5 ]; then
  echo 'usage:' $0 'subscriber_count topic_name sample_count runId logDir' 
  exit 1
fi

subscriber_count=$1
topic_name=$2
sample_count=$3
run_id=$4
log_dir=$5

mkdir -p ~/log/sub

for i in `seq 1 $subscriber_count`;
do
id=$(($i + 100))
echo $id
java -Dlog4j.configurationFile=log4j2.xml -cp build/libs/edgent.jar edu.vanderbilt.edgent.endpoints.subscriber.Subscriber $topic_name $id $sample_count $run_id $log_dir  1>~/log/sub/sub_"$topic_name"_"$i".log 2>&1  & 
  sleep 1
done
