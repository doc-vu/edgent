#!/bin/bash
if [ $# -ne 3 ]; then
  echo 'usage:' $0 'topic_description zk_connector experiment_type' 
  exit 1
fi

topic_description=$1
zk_connector=$2
experiment_type=$3

elems=(${topic_description//:/ })
topic_name=${elems[0]}
publisher_count=${elems[1]}
publication_rate=${elems[2]} 
sample_count=${elems[3]}                
payload_size=${elems[4]}                                  

send_interval= expr 1000 / $publication_rate 
echo $send_interval
