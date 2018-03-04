import itertools,os,sys,time,subprocess
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import metadata,util,argparse
from kazoo.client import KazooClient

#host machines for publishers and subscribers
brokers=['node2']
publisher_test_machines=['node%d'%(i) for i in range(3,8,1)]
subscriber_test_machines=['node%d'%(i) for i in range(8,42,1)]

subscriber_test_machines.remove('node18')
subscriber_test_machines.remove('node23')
subscriber_test_machines.remove('node24')

#maximum number of subscribers that can be created
maximum_subscribers=reduce(lambda x,y:x+y,\
  [metadata.sub_per_hw_type[metadata.get_hw_type(host)] for host in subscriber_test_machines])
#maximum number of publishers that can be created
maximum_publishers=reduce(lambda x,y:x+y,\
  [metadata.pub_per_hw_type[metadata.get_hw_type(host)] for host in publisher_test_machines])

#supported publication rates per publisher
per_publisher_publication_rates=[10]
#supported aggregate publication rates per topic
aggregate_publication_rates=[100]
#publisher distributions
publisher_distributions={
  'r10':['10:1','5:1,1:5','1:10'],
  'r50':['50:1','20:1,6:5','5:10'],
  'r90':['90:1','40:1,2:5,4:10','9:10'],
  'r100':['10:10'],
}
#supported subscription sizes
subscription_size=[50]
#payload size
payload=4000
"""
topic's configuration will be described by a comma separated string:
topic_name,aggregate_rate,publisher_distribution,subscriber_distribution,number_of_publishers,number_of_subscribers,total_endpoints

publisher_distribution is topic_name:number_of_publishers:rate_of_publication:sample_count:payload
subscriber_distribution is topic_name:number_of_subscribers:sample_count
"""

def configurations(number_of_colocated_topics,liveliness):
  foreground_topic=combinations('t1',liveliness)
  background_topic=[combinations('b%d'%(i+1)) for i in range(number_of_colocated_topics)]
  all_topic_combinations=[foreground_topic] + background_topic
  result= [x for x in itertools.product(*all_topic_combinations) if check(x)]
  print(result)
  #with open('configurations_%d'%(number_of_colocated_topics),'w') as f:
  #  for config in result[::-1][0:5]:
  #    f.write(str(config))
  #    f.write('\n')
  return result

#liveliness specifies for how long should a topic exist in the system
def combinations(topic,liveliness=None):
  combinations=[]
  for rate in aggregate_publication_rates:
    for distrib in publisher_distributions['r%d'%(rate)]:
      for sub_size in subscription_size:
        combinations.append(topic_description(topic,rate,distrib,sub_size,liveliness))
  return combinations

def topic_description(name,rate,publisher_distribution,subscription_size,liveliness):
  num_publishers=reduce(lambda x,y:x+y,[int(d.partition(':')[0]) for d in publisher_distribution.split(',')])
  if liveliness:
    publisher_distribution_str='/'.join(['%s:%s:%d:%d'%(name,d,int(d.split(':')[1])*liveliness,payload) for d in publisher_distribution.split(',')])
    subscriber_distribution_str='%s:%d:%d'%(name,subscription_size,
      reduce(lambda x,y:x+y,[int(d.split(':')[0])*int(d.split(':')[1])*liveliness for d in publisher_distribution.split(',')]))
  else:
    publisher_distribution_str='/'.join(['%s:%s:%d:%d'%(name,d,-1,payload) for d in publisher_distribution.split(',')])
    subscriber_distribution_str='%s:%d:%d'%(name,subscription_size,-1)
  return '%s,%d,%s,%s,%d,%d,%d'%(name,rate,publisher_distribution_str,subscriber_distribution_str,num_publishers,subscription_size,num_publishers+subscription_size)
  
def check(configuration):
  num_publishers=reduce(lambda x,y:x+y,[int(t.split(',')[4]) for t in configuration ])
  num_subscribers=reduce(lambda x,y:x+y,[int(t.split(',')[3].split(':')[1]) for t in configuration ])
  expected_nw_utilization_bytes=metadata.payload_size*reduce(lambda x,y:x+y,\
    [int(t.split(',')[1])*(1+int(t.split(',')[3].split(':')[1])) for t in configuration])
  expected_nw_utilization_gb=expected_nw_utilization_bytes*.000000008
  if(num_publishers>maximum_publishers):
    return False
  if(num_subscribers>maximum_subscribers):
    return False
  if(expected_nw_utilization_gb>metadata.maximum_nw_util_gb):
    return False
  return True

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
      topic,num_sub,sample_count=mapping.split(':')
      count=int(num_sub)
    if(endpoint_type=='pub'):
      topic,num_pub,rate,sample_count,payload=mapping.split(':')
      count=int(num_pub)
    
    while(count!=0):
      host=hosts[0]
      capacity=host_capacity_map[host]
      if(count<=capacity):
        if(endpoint_type=='sub'):
          if host in placement:
            placement[host].append('%s:%d:%s'%(topic,count,sample_count))
          else:
            placement[host]=['%s:%d:%s'%(topic,count,sample_count)]
        if(endpoint_type=='pub'):
          if host in placement:
            placement[host].append('%s:%d:%s:%s:%s'%(topic,count,rate,sample_count,payload))
          else:
            placement[host]=['%s:%d:%s:%s:%s'%(topic,count,rate,sample_count,payload)]
        host_capacity_map[host]=host_capacity_map[host]-count
        if(host_capacity_map[host]==0):
          hosts.pop(0)
        count=0
      else:
        if(endpoint_type=='sub'):
          if host in placement:
            placement[host].append('%s:%d:%s'%(topic,capacity,sample_count))
          else:
            placement[host]=['%s:%d:%s'%(topic,capacity,sample_count)]
        if(endpoint_type=='pub'):
          if host in placement:
            placement[host].append('%s:%d:%s:%s:%s'%(topic,capacity,rate,sample_count,payload))
          else:
            placement[host]=['%s:%d:%s:%s:%s'%(topic,capacity,rate,sample_count,payload)]
        host_capacity_map[host]=0
        hosts.pop(0)
        count=count-capacity
  return placement
        
def model_features(config):
  foreground_topic_description= config[0]
  background_topic_description= config[1:]
  x1_foreground_topic_num_pub=foreground_topic_description.split(',')[4]
  x2_foreground_topic_num_sub=foreground_topic_description.split(',')[5]
  x3_foreground_topic_rate=foreground_topic_description.split(',')[1]
  x4_colocated_topics=len(background_topic_description)
  x5_background_num_endpoints=reduce(lambda x,y:x+y,[int(desc.split(',')[6]) for desc in background_topic_description])
  x6_background_rate=reduce(lambda x,y:x+y,[int(desc.split(',')[1]) for desc in background_topic_description])
  return '%s,%s,%s,%d,%d,%d'%(x1_foreground_topic_num_pub,x2_foreground_topic_num_sub,
    x3_foreground_topic_rate,x4_colocated_topics,
    x5_background_num_endpoints,x6_background_rate)

def clean(host_machines):
  #kill existing client endpoints and monitor processes
  util.kill(pattern='edgent.endpoints,monitor',hosts=host_machines)
  #clean logs directory
  util.clean_logs(hosts=host_machines)
  #clean memory
  util.clean_memory(hosts=host_machines)

def partition_foreground_background(placement):
  foreground_background={'foreground':{},'background':{}}
  for host,topic_descriptions in placement.items():
    for topic_description in topic_descriptions:
      if topic_description.startswith('t'):
        if host in foreground_background['foreground']:
          foreground_background['foreground'][host].append(topic_description)
        else:
          foreground_background['foreground'][host]=[topic_description]
      else:
        if host in foreground_background['background']:
          foreground_background['background'][host].append(topic_description)
        else:
          foreground_background['background'][host]=[topic_description]
  return foreground_background

def run(colocated_topics,liveliness):
  zk=KazooClient(hosts=metadata.public_zk)
  zk.start()

  log_dir='%s/colocated_topics_%d'%(metadata.local_log_dir,colocated_topics)
  run_id=0
  configurations(colocated_topics,liveliness)
  #for config in configurations(colocated_topics,liveliness):
  #  run_id+=1
  #  zk.set('/runid','%s'%(run_id))
  #  
  #  #get publisher and subscriber placement 
  #  subscribers=['%s'%(val.split(',')[3]) for val in config]
  #  subscriber_placement= place('sub',subscribers)
  #  subscriber_host_machines=subscriber_placement.keys()
  #  foreground_background_sub= partition_foreground_background(subscriber_placement)
  #   
  #  publishers=reduce(lambda x,y:x+y ,[val.split(',')[2].split('/') for val in config])
  #  publisher_placement= place('pub',publishers)
  #  publisher_host_machines=publisher_placement.keys()
  #  foreground_background_pub= partition_foreground_background(publisher_placement)

  #  #clean-up before running any test
  #  clean(','.join(subscriber_host_machines+publisher_host_machines+brokers))

  #  #start background publishers and subscribers
  #  print("Background subscribers:{}".format(foreground_background_sub['background']))
  #  print("Background publishers:{}".format(foreground_background_pub['background']))
  #  background_load=util.Coordinate("load",run_id,foreground_background_sub['background'],\
  #    foreground_background_pub['background'])
  #  background_load.start()
  #  background_load.stop()

  #  #wait for sometime before starting experiment
  #  time.sleep(20) 
  # 
  #  #start the experiment and wait for it to finish
  #  print("Foreground subscribers:{}".format(foreground_background_sub['foreground']))
  #  print("Foreground publishers:{}".format(foreground_background_pub['foreground']))
  #  foreground_load=util.Coordinate("test",run_id,foreground_background_sub['foreground'],\
  #    foreground_background_pub['foreground'])
  #  util.start_monitors(run_id,"test",0,','.join(brokers))
  #  foreground_load.start()
  #  foreground_load.wait()
  #  foreground_load.stop()

  #  #collect logs
  #  client_and_broker_machines=[]
  #  map(lambda x: client_and_broker_machines.append(x),brokers) 
  #  map(lambda x: client_and_broker_machines.append(x),foreground_background_sub['foreground'].keys()) 
  #  util.collect_logs(run_id,log_dir,','.join(client_and_broker_machines))

  #  #copy model features 
  #  features=model_features(config)
  #  with open('%s/%s/features'%(log_dir,run_id),'w') as f:
  #    f.write('foreground_#pub,foreground_#sub,foreground_rate,#colocated_topics,background_#endpoints,background_rate\n')
  #    f.write(features+'\n')

  #  #kill publishers and subscribers before exiting
  #  util.kill(pattern='edgent.endpoints',hosts=','.join(publisher_host_machines + subscriber_host_machines))

  #  #summarize results
  #  subprocess.check_call(['python','src/plots/summarize/summarize.py',\
  #    '-log_dir',log_dir,'-sub_dirs',str(run_id)])
  #zk.stop()

  
if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for running co-located topics test')
  parser.add_argument('colocated',type=int,help='number of colocated topics')
  parser.add_argument('liveliness',type=int,help='time in seconds for which foreground topic should send data')
  args=parser.parse_args()

  #run test 
  run(args.colocated,args.liveliness)
