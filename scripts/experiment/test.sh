#!/bin/bash

#kill all existing end-points 
cd /home/kharesp/workspace/java/edgent/scripts/ansible
ansible-playbook playbooks/util/kill.yml --extra-vars="pattern=edgent.endpoints"

#restart broker
ansible-playbook playbooks/experiment/eb.yml  --limit node2\
    --extra-vars="zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 io_threads=1"

#start test
cd /home/kharesp/workspace/java/edgent/scripts/experiment
python src/tests/publisher_stress_test.py  test 1 1 1 1 1000 4000 1000 ~/log

#summarize results
python src/plots/summarize/summarize.py  -log_dir ~/log -sub_dir 1

mv ~/log/1 ~/log/rate_1



#kill all existing end-points 
cd /home/kharesp/workspace/java/edgent/scripts/ansible
ansible-playbook playbooks/util/kill.yml --extra-vars="pattern=edgent.endpoints"

#restart broker
ansible-playbook playbooks/experiment/eb.yml  --limit node2\
    --extra-vars="zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 io_threads=1"

#start test
cd /home/kharesp/workspace/java/edgent/scripts/experiment
python src/tests/publisher_stress_test.py  test 1 1 1 1 100 4000 1000 ~/log

#summarize results
python src/plots/summarize/summarize.py  -log_dir ~/log -sub_dir 1

mv ~/log/1 ~/log/rate_10

#kill all existing end-points 
cd /home/kharesp/workspace/java/edgent/scripts/ansible
ansible-playbook playbooks/util/kill.yml --extra-vars="pattern=edgent.endpoints"

#restart broker
ansible-playbook playbooks/experiment/eb.yml  --limit node2\
    --extra-vars="zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 io_threads=1"

#start test
cd /home/kharesp/workspace/java/edgent/scripts/experiment
python src/tests/publisher_stress_test.py  test 1 1 1 1 50 4000 1000 ~/log

#summarize results
python src/plots/summarize/summarize.py  -log_dir ~/log -sub_dir 1

mv ~/log/1 ~/log/rate_20


#kill all existing end-points 
cd /home/kharesp/workspace/java/edgent/scripts/ansible
ansible-playbook playbooks/util/kill.yml --extra-vars="pattern=edgent.endpoints"

#restart broker
ansible-playbook playbooks/experiment/eb.yml  --limit node2\
    --extra-vars="zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 io_threads=1"

#start test
cd /home/kharesp/workspace/java/edgent/scripts/experiment
python src/tests/publisher_stress_test.py  test 1 1 1 1 20 4000 1000  ~/log

#summarize results
python src/plots/summarize/summarize.py  -log_dir ~/log -sub_dir 1

mv ~/log/1 ~/log/rate_50


#kill all existing end-points 
cd /home/kharesp/workspace/java/edgent/scripts/ansible
ansible-playbook playbooks/util/kill.yml --extra-vars="pattern=edgent.endpoints"

#restart broker
ansible-playbook playbooks/experiment/eb.yml  --limit node2\
    --extra-vars="zk_connector=129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181 io_threads=1"

#start test
cd /home/kharesp/workspace/java/edgent/scripts/experiment
python src/tests/publisher_stress_test.py  test 1 1 1 1 10 4000 1000  ~/log

#summarize results
python src/plots/summarize/summarize.py  -log_dir ~/log -sub_dir 1

mv ~/log/1 ~/log/rate_100
