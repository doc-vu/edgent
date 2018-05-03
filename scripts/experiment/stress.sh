#!/bin/bash

#kill all existing end-points before starting the test
#cd /home/kharesp/workspace/java/edgent/scripts/ansible
#ansible-playbook playbooks/util/kill.yml --extra-vars="pattern=edgent.endpoints"

processing_intervals=(
  50
)

subscription_sizes=(
  10
  20
  40
  80
  160
  320
)


for interval in "${processing_intervals[@]}"
do
  cd /home/kharesp/workspace/java/edgent/scripts/ansible
  #kill EdgeBroker
  ansible-playbook playbooks/util/kill.yml --limit node2 --extra-vars="pattern=EdgeBroker"

  sleep 5 
  #restart broker
  arguments="zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 io_threads=3 id=0 per_sample_processing_interval=${interval} cpu_load=1"
  ansible-playbook playbooks/experiment/eb.yml  --limit node2\
      --extra-vars="$arguments"
  
  #start test
  cd /home/kharesp/workspace/java/edgent/scripts/experiment
  python src/tests/debs_load_test.py -run_id $interval -region_cars r1:10 r2:10 r3:10 r4:10 r5:10 r6:10 r7:10
done

#for interval in "${processing_intervals[@]}"
#do
#  for subscription_size in "${subscription_sizes[@]}"
#  do
#    cd /home/kharesp/workspace/java/edgent/scripts/ansible
#    #kill EdgeBroker
#    ansible-playbook playbooks/util/kill.yml --limit node2 --extra-vars="pattern=EdgeBroker"
#
#    sleep 5 
#    #restart broker
#    arguments="zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 io_threads=3 id=0 per_sample_processing_interval=${interval} cpu_load=1"
#    ansible-playbook playbooks/experiment/eb.yml  --limit node2\
#        --extra-vars="$arguments"
#    
#    #start test
#    cd /home/kharesp/workspace/java/edgent/scripts/experiment
#    python src/tests/debs_load_test.py -run_id $subscription_size -region_cars r1:$subscription_size
#  done
#  cd /home/kharesp/log
#  mkdir load_${interval}
#  mv 10 20 40 80 160 320  load_${interval}
#done
