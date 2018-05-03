#!/bin/bash

mkdir -p ~/log/sub

subscriber_count=125

for i in `seq 1 $subscriber_count`;
do
  java -cp build/libs/edgent.jar edu.vanderbilt.edgent.brokers.TestReceiver   1>~/log/sub/sub_"$i".log 2>&1  & 
  sleep 1
done
