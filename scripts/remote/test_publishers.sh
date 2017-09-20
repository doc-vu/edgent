#!/bin/bash

if [ $# -ne 4 ]; then
  echo 'usage:' $0 'publisher_count topic_name sample_count send_interval' 
  exit 1
fi

publisher_count=$1
topic_name=$2
sample_count=$3
send_interval=$4

mkdir -p ~/log/pub

for i in `seq 1 $publisher_count`;
do
id=$(($i + 100))
echo $id
java -Dlog4j.configurationFile=log4j2.xml -cp build/libs/edgent.jar edu.vanderbilt.edgent.endpoints.publisher.Publisher $topic_name $id $sample_count $send_interval  1>~/log/pub/pub_"$topic_name"_"$i".log 2>&1  & 
  sleep 1
done
