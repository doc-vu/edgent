import numpy as np
import os,sys,time,random,argparse,subprocess,json
from functools import reduce
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import metadata,util

##########################################################################
##########################################################################
##########################################################################
#test machine configuration
brokers= ['node2']
subscriber_test_machines= ['node3','node4']
publisher_test_machines= ['node7','node8','node9','node10','node11',\
  'node12','node13','node14','node15','node16']

node_mq_map={
  'node3': 'tcp://10.20.30.1:1026',
  'node4': 'tcp://10.20.30.1:2026',
  'node7': 'tcp://10.20.30.1:1026',
  'node8': 'tcp://10.20.30.1:1026',
  'node9': 'tcp://10.20.30.1:1026',
  'node10': 'tcp://10.20.30.1:1026',
  'node11': 'tcp://10.20.30.1:1026',
  'node12': 'tcp://10.20.30.1:2026',
  'node13': 'tcp://10.20.30.1:2026',
  'node14': 'tcp://10.20.30.1:2026',
  'node15': 'tcp://10.20.30.1:2026',
  'node16': 'tcp://10.20.30.1:2026',
}
#for m in subscriber_test_machines:
#  publisher_test_machines.remove(m)

#maximum number of publishers 
maximum_publishers=reduce(lambda x,y:x+y,\
  [metadata.pub_per_hw_type[metadata.get_hw_type(host)] for host in publisher_test_machines])

#maximum number of subscribers
maximum_subscribers=reduce(lambda x,y:x+y,\
  [metadata.sub_per_hw_type[metadata.get_hw_type(host)] for host in subscriber_test_machines])


#maximum publication rate supported for a given per-topic processing interval
max_supported_rates={
  10:78,
  20:37,
  30:24,
  40:20,
}
##########################################################################
##########################################################################
##########################################################################
#Test parameters
payload=4000
per_publisher_publication_rate=1
subscribers_per_topic=1
liveliness=120
rates=10
processing_intervals=[10,20,30,40] #milliseconds
range_of_rates={ #msg/sec
  10:[int(v) for v in np.linspace(1,max_supported_rates[10],rates)],
  20:[int(v) for v in np.linspace(1,max_supported_rates[20],rates)],
  30:[int(v) for v in np.linspace(1,max_supported_rates[30],rates)],
  40:[int(v) for v in np.linspace(1,max_supported_rates[40],rates)],
}
##########################################################################
##########################################################################
##########################################################################
def pattern_with_set_foreground(topics,foreground_processing_interval,foreground_rate):
  p='t1:%d:%d,'%(foreground_processing_interval,foreground_rate)
  for i in range(1,topics):
    interval= processing_intervals[random.randint(0,len(processing_intervals)-1)]
    rate= range_of_rates[interval][random.randint(0,len(range_of_rates[interval])-1)]
    p=p+'t%d:%d:%d,'%(i+1,interval,rate)
  return p.rstrip(',')

def pattern(topics):
  p=''
  for i in range(topics):
    #randomly select a processing interval and rate for the topic
    interval= processing_intervals[random.randint(0,len(processing_intervals)-1)]
    rate= range_of_rates[interval][random.randint(0,len(range_of_rates[interval])-1)]
    p=p+'t%d:%d:%d,'%(i+1,interval,rate)
  return p.rstrip(',')

def check_correctness(pattern):
  total_publishers=reduce(lambda x,y:x+y,[int(t.split(':')[2]) for t in pattern.split(',')])
  if total_publishers <= maximum_publishers:
    return True
  else:
    return False

def combinations_with_set_foreground(topics,count,foreground_processing_interval,foreground_rate):
  res=set()
  while(len(res) < count):
    p= pattern_with_set_foreground(topics,foreground_processing_interval,foreground_rate)
    if(check_correctness(p)):
      res.add(p)
  return res

def combinations(topics,count):
  res=set()
  while(len(res) < count):
    p= pattern(topics)
    if(check_correctness(p)):
      res.add(p)
  return res

def write_configuration(config,file_path):
  #write test configuration
  with open(file_path,'w') as f:
    #topic_configuration= name,processing_interval,rate,pub_distribution,sub_distribution,#pub,#sub,#endpoints,payload,interval*rate
    f.write('topic_name,processing_interval,aggregate_rate,publisher_distribution,subscriber_distribution,number_of_publishers,number_of_subscribers,total_endpoints,payload,interval*rate\n')
    for topic_description in config:
      f.write(topic_description+'\n') 

def load_configuration(config_path):
  config=[]
  with open(config_path,'r') as f:
    #skip header
    next(f)
    for line in f:
      config.append(line.rstrip())
  return config 

def create_configuration(iteration):
  config=[]
  for t in iteration.split(','):
    topic,processing_interval,rate=t.split(':')  
    num_publishers=int(rate)/per_publisher_publication_rate
    #pub_distribution=topic:#pub:rate:sample_count:payload:interval
    pub_distribution='%s:%d:%d:%d:%d:%s'%(topic,
      num_publishers,
      per_publisher_publication_rate,
      liveliness,
      payload,
      processing_interval)
    #sub_distribution=topic:#sub:sample_count:interval
    sub_distribution='%s:%d:%d:%s'%(topic,
      subscribers_per_topic,
      liveliness*num_publishers,
      processing_interval)
    #topic_configuration= name,processing_interval,rate,pub_distribution,sub_distribution,#pub,#sub,#endpoints,payload,interval*rate
    topic_config='%s,%s,%s,%s,%s,%d,%d,%d,%d,%f'%(topic,
      processing_interval,
      rate,
      pub_distribution,
      sub_distribution,
      num_publishers,
      subscribers_per_topic,
      num_publishers+subscribers_per_topic,
      payload,
      (int(processing_interval)*int(rate))/1000.0)
    
    config.append(topic_config)
  return config
  
def place(endpoint_type,endpoint_distribution):
  placement={}
  if(endpoint_type=='sub'):
    hosts=list(subscriber_test_machines)
    host_capacity_map={h:metadata.get_host_subscriber_capacity(h)\
      for h in subscriber_test_machines}
  elif(endpoint_type=='pub'):
    hosts=list(publisher_test_machines)
    host_capacity_map={h:metadata.get_host_publisher_capacity(h)\
      for h in publisher_test_machines}
  else:
    print('endpoint_type:%s is invalid'%(endpoint_type))
    return

  for mapping in endpoint_distribution:
    if(endpoint_type=='sub'):
      #sub_distribution=topic:#sub:sample_count:interval
      topic,num_sub,sample_count,processing_interval=mapping.split(':')
      count=int(num_sub)
    if(endpoint_type=='pub'):
      #pub_distribution=topic:#pub:rate:sample_count:payload:interval
      topic,num_pub,rate,sample_count,payload,processing_interval=mapping.split(':')
      count=int(num_pub)
   
    while(count!=0):
      host=hosts[0]
      capacity=host_capacity_map[host]
      if(count<=capacity):
        if(endpoint_type=='sub'):
          if host in placement:
            placement[host].append('%s:%d:%s:%s'%(topic,count,
              sample_count,processing_interval))
          else:
            placement[host]=['%s:%d:%s:%s'%(topic,count,
              sample_count,processing_interval)]
        if(endpoint_type=='pub'):
          if host in placement:
            placement[host].append('%s:%d:%s:%s:%s:%s'%(topic,count,
              rate,sample_count,payload,processing_interval))
          else:
            placement[host]=['%s:%d:%s:%s:%s:%s'%(topic,count,
              rate,sample_count,payload,processing_interval)]

        host_capacity_map[host]=host_capacity_map[host]-count
        if(host_capacity_map[host]==0):
          hosts.pop(0)
        count=0
      else:
        if(endpoint_type=='sub'):
          if host in placement:
            placement[host].append('%s:%d:%s:%s'%(topic,capacity,
              sample_count,processing_interval))
          else:
            placement[host]=['%s:%d:%s:%s'%(topic,capacity,
              sample_count,processing_interval)]
        if(endpoint_type=='pub'):
          if host in placement:
            placement[host].append('%s:%d:%s:%s:%s:%s'%(topic,capacity,
              rate,sample_count,payload,processing_interval))
          else:
            placement[host]=['%s:%d:%s:%s:%s:%s'%(topic,capacity,
              rate,sample_count,payload,processing_interval)]
        host_capacity_map[host]=0
        hosts.pop(0)
        count=count-capacity
  return placement

def model_features(config):
  res={}
  for tdesc in config:
    parts= tdesc.split(',')
    topic_name=parts[0]
    processing_interval=parts[1]
    publication_rate=parts[2]
    background_rate_x_processing_interval=0
    for tdesc in config:
      other_topic_name=tdesc.split(',')[0]
      if not other_topic_name==topic_name:
        background_rate_x_processing_interval+= float(tdesc.split(',')[-1])
        
    res[topic_name]='%s,%s,%f'%(processing_interval,publication_rate,background_rate_x_processing_interval)
  return res
  
def model_output(log_dir,run_id):
  toGb=.000008
  res={}
  with open('%s/%s/summary/summary_util.csv'%(log_dir,run_id),'r') as f:
    #skip header:hostname,avg_cpu(%),avg_iowait(%),avg_mem(gb),avg_nw(kB/sec)
    next(f)
    hostname,cpu,iowait,mem,nw= next(f).split(',')
    res['cpu']=float(cpu)
    res['mem']=float(mem)
    res['nw']=float(nw)*toGb

  res['90th']={}
  res['avg']={}
  with open('%s/%s/summary/summary_topic.csv'%(log_dir,run_id),'r') as f:
    #skip header:
    next(f)
    for line in f:
      parts=line.split(',')
      res['90th'][parts[0]]=float(parts[9])
      res['avg'][parts[0]]=float(parts[2])
  return res

def experiment(log_dir,run_id,config,subscriber_placement,publisher_placement,zk_connector,fe_address,mq_connector):
  #clean-up before running any test
  print("\n\nCleaning logs directory")
  util.clean_logs(','.join(list(subscriber_placement.keys())+\
    list(publisher_placement.keys())+brokers))

  #restart edge-broker
  print("\n\nRestarting EdgeBroker")
  print(zk_connector)
  util.start_eb(','.join(brokers),zk_connector) 

  #wait for EB to initialize
  time.sleep(10)

  #start the experiment and wait for it to finish
  print('\n\n\nStarting test endpoints')
  print("Subscribers:{}".format(subscriber_placement))
  print("Publishers:{}".format(publisher_placement))
  load=util.Coordinate("test",run_id,subscriber_placement,\
    publisher_placement,brokers,log_dir,True,zk_connector,fe_address,mq_connector)

  #start monitoring processes
  util.start_monitors(run_id,"test",0,','.join(brokers),zk_connector)
  
  load.run()

  #write test configuration
  write_configuration(config,'%s/%s/config'%(log_dir,run_id))

  #summarize results
  print("\n\nSummarizing results")
  subprocess.check_call(['python','src/plots/summarize/summarize.py',\
    '-log_dir',log_dir,'-sub_dirs',str(run_id)])

def run(config,log_dir,run_id,\
  zk_connector,fe_address,mq_connector):
  #get subscriber configurations for all topics in this test config
  subscribers=['%s'%(tdesc.split(',')[4]) for tdesc in config]
  #get publisher configurations for all topics in this test config
  publishers=['%s'%(tdesc.split(',')[3]) for tdesc in config]

  #get placement for all subscribers in this test
  subscriber_placement=place('sub',subscribers)
  #get placement for all publishers in this test
  publisher_placement=place('pub',publishers)
  #run experiment
  experiment(log_dir,run_id,config,subscriber_placement,publisher_placement,zk_connector,fe_address,mq_connector)

if __name__ == "__main__":
  parser= argparse.ArgumentParser(description='script for running test')
  parser.add_argument('-config',required=True)
  parser.add_argument('-log_dir',required=True)
  parser.add_argument('-run_id',type=int,required=True)
  parser.add_argument('-zk_connector',required=True)
  parser.add_argument('-fe_address',required=True)
  parser.add_argument('-mq_connector',required=True)
  args=parser.parse_args()

  #run experiment
  run(json.loads(args.config),args.log_dir,\
    args.run_id,args.zk_connector,\
    args.fe_address,args.mq_connector)
