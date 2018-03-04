#!/bin/bash

#####Test Case-1000
#cd /home/kharesp/workspace/java/edgent/scripts/ansible
##kill all existing end-points 
#ansible-playbook playbooks/util/kill.yml --extra-vars="pattern=edgent.endpoints"
##restart broker
#ansible-playbook playbooks/experiment/eb.yml  --limit node2\
#    --extra-vars="zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 io_threads=1"
#
#cd /home/kharesp/workspace/java/edgent/scripts/experiment
##start test
#python src/tests/publisher_stress_test.py  test 1000 1 1000 1 1000 4000 60 ~/log
##summarize results
#python src/plots/summarize/summarize.py  -log_dir ~/log -sub_dir 1000


###Test Case-900
cd /home/kharesp/workspace/java/edgent/scripts/ansible
#kill all existing end-points 
ansible-playbook playbooks/util/kill.yml --extra-vars="pattern=edgent.endpoints"
#restart broker
ansible-playbook playbooks/experiment/eb.yml  --limit node2\
    --extra-vars="zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 io_threads=1"

##start mute publishers
#ansible-playbook playbooks/experiment/publisher.yml  --limit node14\
#    --extra-vars="topic_count_map={'t1':100} sample_count=12 send_interval=10000 payload_size=4000 zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 \
#    experiment_type=load send=0"

cd /home/kharesp/workspace/java/edgent/scripts/experiment
#start test
python src/tests/publisher_stress_test.py  test 900 1 900 1 1000 4000 60 ~/log
#summarize results
python src/plots/summarize/summarize.py  -log_dir ~/log -sub_dir 900

###Test Case-800
cd /home/kharesp/workspace/java/edgent/scripts/ansible
#kill all existing end-points 
ansible-playbook playbooks/util/kill.yml --extra-vars="pattern=edgent.endpoints"
#restart broker
ansible-playbook playbooks/experiment/eb.yml  --limit node2\
    --extra-vars="zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 io_threads=1"

###start mute publishers
#ansible-playbook playbooks/experiment/publisher.yml  --limit node14,node15\
#    --extra-vars="topic_count_map={'t1':100} sample_count=12 send_interval=10000 payload_size=4000 zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 \
#    experiment_type=load send=0"

cd /home/kharesp/workspace/java/edgent/scripts/experiment
#start test
python src/tests/publisher_stress_test.py  test 800 1 800 1 1000 4000 60 ~/log
#summarize results
python src/plots/summarize/summarize.py  -log_dir ~/log -sub_dir 800

###Test Case-700
cd /home/kharesp/workspace/java/edgent/scripts/ansible
#kill all existing end-points 
ansible-playbook playbooks/util/kill.yml --extra-vars="pattern=edgent.endpoints"
#restart broker
ansible-playbook playbooks/experiment/eb.yml  --limit node2\
    --extra-vars="zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 io_threads=1"

###start mute publishers
#ansible-playbook playbooks/experiment/publisher.yml  --limit node14,node15,node16\
#    --extra-vars="topic_count_map={'t1':100} sample_count=12 send_interval=10000 payload_size=4000 zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 \
#    experiment_type=load send=0"

cd /home/kharesp/workspace/java/edgent/scripts/experiment
#start test
python src/tests/publisher_stress_test.py  test 700 1 700 1 1000 4000 60 ~/log
#summarize results
python src/plots/summarize/summarize.py  -log_dir ~/log -sub_dir 700

###Test Case-600
cd /home/kharesp/workspace/java/edgent/scripts/ansible
#kill all existing end-points 
ansible-playbook playbooks/util/kill.yml --extra-vars="pattern=edgent.endpoints"
#restart broker
ansible-playbook playbooks/experiment/eb.yml  --limit node2\
    --extra-vars="zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 io_threads=1"

###start mute publishers
#ansible-playbook playbooks/experiment/publisher.yml  --limit node14,node15,node16,node17\
#    --extra-vars="topic_count_map={'t1':100} sample_count=12 send_interval=10000 payload_size=4000 zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 \
#    experiment_type=load send=0"

cd /home/kharesp/workspace/java/edgent/scripts/experiment
#start test
python src/tests/publisher_stress_test.py  test 600 1 600 1 1000 4000 60 ~/log
#summarize results
python src/plots/summarize/summarize.py  -log_dir ~/log -sub_dir 600

###Test Case-500
cd /home/kharesp/workspace/java/edgent/scripts/ansible
#kill all existing end-points 
ansible-playbook playbooks/util/kill.yml --extra-vars="pattern=edgent.endpoints"
#restart broker
ansible-playbook playbooks/experiment/eb.yml  --limit node2\
    --extra-vars="zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 io_threads=1"

###start mute publishers
#ansible-playbook playbooks/experiment/publisher.yml  --limit node14,node15,node16,node17,node18\
#    --extra-vars="topic_count_map={'t1':100} sample_count=12 send_interval=10000 payload_size=4000 zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 \
#    experiment_type=load send=0"

cd /home/kharesp/workspace/java/edgent/scripts/experiment
#start test
python src/tests/publisher_stress_test.py  test 500 1 500 1 1000 4000 60 ~/log
#summarize results
python src/plots/summarize/summarize.py  -log_dir ~/log -sub_dir 500

###Test Case-400
cd /home/kharesp/workspace/java/edgent/scripts/ansible
#kill all existing end-points 
ansible-playbook playbooks/util/kill.yml --extra-vars="pattern=edgent.endpoints"
#restart broker
ansible-playbook playbooks/experiment/eb.yml  --limit node2\
    --extra-vars="zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 io_threads=1"

###start mute publishers
#ansible-playbook playbooks/experiment/publisher.yml  --limit node14,node15,node16,node17,node18,node19\
#    --extra-vars="topic_count_map={'t1':100} sample_count=12 send_interval=10000 payload_size=4000 zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 \
#    experiment_type=load send=0"

cd /home/kharesp/workspace/java/edgent/scripts/experiment
#start test
python src/tests/publisher_stress_test.py  test 400 1 400 1 1000 4000 60 ~/log
#summarize results
python src/plots/summarize/summarize.py  -log_dir ~/log -sub_dir 400

###Test Case-300
cd /home/kharesp/workspace/java/edgent/scripts/ansible
#kill all existing end-points 
ansible-playbook playbooks/util/kill.yml --extra-vars="pattern=edgent.endpoints"
#restart broker
ansible-playbook playbooks/experiment/eb.yml  --limit node2\
    --extra-vars="zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 io_threads=1"

###start mute publishers
#ansible-playbook playbooks/experiment/publisher.yml  --limit node14,node15,node16,node17,node18,node19,node20\
#    --extra-vars="topic_count_map={'t1':100} sample_count=12 send_interval=10000 payload_size=4000 zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 \
#    experiment_type=load send=0"

cd /home/kharesp/workspace/java/edgent/scripts/experiment
#start test
python src/tests/publisher_stress_test.py  test 300 1 300 1 1000 4000 60 ~/log
#summarize results
python src/plots/summarize/summarize.py  -log_dir ~/log -sub_dir 300

###Test Case-200
cd /home/kharesp/workspace/java/edgent/scripts/ansible
#kill all existing end-points 
ansible-playbook playbooks/util/kill.yml --extra-vars="pattern=edgent.endpoints"
#restart broker
ansible-playbook playbooks/experiment/eb.yml  --limit node2\
    --extra-vars="zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 io_threads=1"

###start mute publishers
#ansible-playbook playbooks/experiment/publisher.yml  --limit node14,node15,node16,node17,node18,node19,node20,node21\
#    --extra-vars="topic_count_map={'t1':100} sample_count=12 send_interval=10000 payload_size=4000 zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 \
#    experiment_type=load send=0"

cd /home/kharesp/workspace/java/edgent/scripts/experiment
#start test
python src/tests/publisher_stress_test.py  test 200 1 200 1 1000 4000 60 ~/log
#summarize results
python src/plots/summarize/summarize.py  -log_dir ~/log -sub_dir 200

###Test Case-100
cd /home/kharesp/workspace/java/edgent/scripts/ansible
#kill all existing end-points 
ansible-playbook playbooks/util/kill.yml --extra-vars="pattern=edgent.endpoints"
#restart broker
ansible-playbook playbooks/experiment/eb.yml  --limit node2\
    --extra-vars="zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 io_threads=1"

###start mute publishers
#ansible-playbook playbooks/experiment/publisher.yml  --limit node14,node15,node16,node17,node18,node19,node20,node21,node22\
#    --extra-vars="topic_count_map={'t1':100} sample_count=12 send_interval=10000 payload_size=4000 zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 \
#    experiment_type=load send=0"

cd /home/kharesp/workspace/java/edgent/scripts/experiment
#start test
python src/tests/publisher_stress_test.py  test 100 1 100 1 1000 4000 60 ~/log
#summarize results
python src/plots/summarize/summarize.py  -log_dir ~/log -sub_dir 100
