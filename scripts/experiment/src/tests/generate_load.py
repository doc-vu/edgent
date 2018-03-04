import argparse,os,sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import subprocess,metadata

#Script for starting a designated number of loading topics
#nodes used for running loading processes 
nodes=['node5','node6','node7','node8','node9','node10','node11','node12','node13','node14','node15','node16']

def launch_sub(host,topic_count_map,sample_count,run_id):
  command_string='cd %s && ansible-playbook playbooks/experiment/subscriber.yml --limit %s\
    --extra-vars="topic_count_map=%s run_id=%s sample_count=%s"'%\
    (metadata.ansible,host,str(topic_count_map).replace(" ",""),run_id,sample_count)
  subprocess.check_call(['bash','-c',command_string])

def launch_pub(host,topic_count_map,sample_count,send_interval,run_id,payload):
  command_string='cd %s && ansible-playbook playbooks/experiment/publisher.yml  --limit %s\
    --extra-vars="topic_count_map=%s sample_count=%s send_interval=%s payload_size=%s zk_connector=%s"'%\
    (metadata.ansible,host,str(topic_count_map).replace(" ",""),\
    sample_count,send_interval,payload,metadata.zk)
  subprocess.check_call(['bash','-c',command_string])

def load(no_topics):
  number_of_machines=no_topics//metadata.max_topics_per_host
  remaining=no_topics%metadata.max_topics_per_host
  topic_count_map={}
  if(number_of_machines==0):
    for topic in range(remaining):
      topic_count_map['l%d'%(topic+1)]=1
    launch_sub('node10',topic_count_map,-1,-1)
    launch_pub('node9',topic_count_map,-1,25,-1,4000)
  else:
    for i in range(number_of_machines):
      topic_count_map={}
      for t in range(metadata.max_topics_per_host):
        topic_count_map['l%d'%(i*metadata.max_topics_per_host+t+1)]=1  
      launch_sub('node%d'%(10+i*2),topic_count_map,-1,-1) 
      launch_pub('node%d'%(9+i*2),topic_count_map,-1,25,-1,4000)
    if(remaining>0):
      topic_count_map={}
      for t in range(remaining):
        topic_count_map['l%d'%(metadata.max_topics_per_host*number_of_machines+t+1)]=1
      launch_sub('node%d'%(10+2*number_of_machines),topic_count_map,-1,-1)
      launch_pub('node:%d'%(9+2*number_of_machines),topic_count_map,-1,25,-1,4000)

if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for starting loading endpoints')
  parser.add_argument('topics',type=int,help='number of loading topics to start (<90)')
  args=parser.parse_args()
  load(args.topics)
