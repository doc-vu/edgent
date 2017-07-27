import json,subprocess,conf,multiprocessing,metadata
from sys import argv

def launch_pub(host,topic_count_map,sample_count,send_interval,run_id):
  region_id=host.split('-')[0][3:]
  command_string='cd %s && ansible-playbook playbooks/experiment/publisher.yml  --limit %s\
    --extra-vars="topic_count_map=%s region_id=%s run_id=%s sample_count=%s send_interval=%s zk_connector=%s"'%\
    (metadata.ansible,host,str(topic_count_map).replace(" ",""),region_id,run_id,sample_count,send_interval,metadata.zk)
  subprocess.check_call(['bash','-c',command_string])


if __name__=="__main__":
  host_topic_map=json.loads(argv[1])
  sample_count=argv[2]
  send_interval=argv[3]
  run_id=argv[4]
  processes=[]
  for host,topic_count_map in host_topic_map.items():
    processes.append(multiprocessing.Process(target=launch_pub,args=(host,topic_count_map,sample_count,send_interval,run_id)))

  for p in processes:
    p.start()
  for p in processes:
    p.join()
