import json,subprocess,conf,multiprocessing,metadata
from sys import argv

def launch_pub(host,topic_count_map,sample_count,send_interval,run_id,payload,experiment_type):
  command_string='cd %s && ansible-playbook playbooks/experiment/publisher.yml  --limit %s\
    --extra-vars="topic_count_map=%s sample_count=%s send_interval=%s payload_size=%s zk_connector=%s experiment_type=%s send=1"'%\
    (metadata.ansible,host,str(topic_count_map).replace(" ",""),\
    sample_count,send_interval,payload,metadata.public_zk,experiment_type)
  subprocess.check_call(['bash','-c',command_string])


if __name__=="__main__":
  host_topic_map=json.loads(argv[1])
  sample_count=argv[2]
  send_interval=argv[3]
  run_id=argv[4]
  payload=argv[5]
  experiment_type=argv[6]

  processes=[]
  for host,topic_count_map in host_topic_map.items():
    processes.append(multiprocessing.Process(target=launch_pub,args=(host,topic_count_map,sample_count,send_interval,run_id,payload,experiment_type)))

  for p in processes:
    p.start()
  for p in processes:
    p.join()
