import json,subprocess,conf,multiprocessing,metadata
from sys import argv

def launch_sub(host,topic_count_map,sample_count,run_id,experiment_type):
  print('\n\n\nLaunching subscribers on host:%s'%(host))
  command_string='cd %s && ansible-playbook playbooks/experiment/subscriber.yml  --limit %s\
    --extra-vars="topic_count_map=%s run_id=%s sample_count=%s experiment_type=%s"'%\
    (metadata.ansible,host,str(topic_count_map).replace(" ",""),run_id,sample_count,experiment_type)
  subprocess.check_call(['bash','-c',command_string])

if __name__=="__main__":
  host_topic_map=json.loads(argv[1])
  sample_count=argv[2]
  run_id=argv[3]
  experiment_type=argv[4]

  processes=[]
  for host,topic_count_map in host_topic_map.items():
    processes.append(multiprocessing.Process(target=launch_sub,args=(host,topic_count_map,sample_count,run_id,experiment_type)))

  for p in processes:
    p.start()
  for p in processes:
    p.join()
