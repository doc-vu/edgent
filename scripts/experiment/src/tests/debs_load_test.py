import argparse,os,sys,time,subprocess
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import metadata,util

#brokers=['node%d'%(i) for i in range(2,7,1)]
brokers=['node2']
publisher_test_machines=['node%d'%(i) for i in range(3,16,1)]
publisher_test_machines+=['node28','node29','node30','node31']
subscriber_test_machines=['node%d'%(i) for i in range(16,42,1)]

subscriber_test_machines.remove('node25')
subscriber_test_machines.remove('node27')
subscriber_test_machines.remove('node28')
subscriber_test_machines.remove('node29')
subscriber_test_machines.remove('node30')
subscriber_test_machines.remove('node31')
subscriber_test_machines.remove('node23')
subscriber_test_machines.remove('node34')
subscriber_test_machines.remove('node39')

pub_per_hw_type={'hw1':100,'hw2':200}
sub_per_hw_type={'hw1':50,'hw2':100,'hw3':150,'hw4':200,'hw5':200}

#maximum number of subscribers that can be created
maximum_subscribers=reduce(lambda x,y:x+y,\
  [sub_per_hw_type[metadata.get_hw_type(host)] for host in subscriber_test_machines])
print('Maximum #subscribers:%d'%(maximum_subscribers))

#maximum number of publishers that can be created
maximum_publishers=reduce(lambda x,y:x+y,\
  [pub_per_hw_type[metadata.get_hw_type(host)] for host in publisher_test_machines])
print('Maximum #publishers:%d'%(maximum_publishers))

def get_host_subscriber_capacity(host):
  return sub_per_hw_type[metadata.get_hw_type(host)]

def get_host_publisher_capacity(host):
  return pub_per_hw_type[metadata.get_hw_type(host)]

def check(configuration):
  num_publishers=reduce(lambda x,y:x+y,[int(t.split(',')[4]) for t in configuration ])
  num_subscribers=reduce(lambda x,y:x+y,[int(t.split(',')[5]) for t in configuration ])
  expected_nw_utilization_bytes=reduce(lambda x,y:x+y,\
    [int(t.split(',')[7])*int(t.split(',')[1])*(1+int(t.split(',')[5])) for \
      t in configuration])
  expected_nw_utilization_gb=expected_nw_utilization_bytes*.000000008
  if(num_publishers>maximum_publishers):
    print('Exceeded maximum #publishers')
    return False
  if(num_subscribers>maximum_subscribers):
    print('Exceeded maximum #subscribers')
    return False
  if(expected_nw_utilization_gb>metadata.maximum_nw_util_gb):
    print('Exceeded nw threshold')
    return False
  return True

def place(endpoint_type,endpoint_distribution):
  placement={}
  if(endpoint_type=='sub'):
    hosts=list(subscriber_test_machines)
    host_capacity_map={h:get_host_subscriber_capacity(h)\
      for h in subscriber_test_machines}
  elif(endpoint_type=='pub'):
    hosts=list(publisher_test_machines)
    host_capacity_map={h:get_host_publisher_capacity(h)\
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

def model_features(config):
  foreground_topic_description= config[0]
  background_topic_description= config[1:]
  x1_foreground_topic_num_pub=foreground_topic_description.split(',')[4]
  x2_foreground_topic_num_sub=foreground_topic_description.split(',')[5]
  x3_foreground_topic_rate=foreground_topic_description.split(',')[1]
  x4_colocated_topics=len(background_topic_description)
  x5_background_num_pub=reduce(lambda x,y:x+y,[int(desc.split(',')[4]) for desc in background_topic_description])
  x6_background_num_sub=reduce(lambda x,y:x+y,[int(desc.split(',')[5]) for desc in background_topic_description])
  x7_background_rate=reduce(lambda x,y:x+y,[int(desc.split(',')[1]) for desc in background_topic_description])
  return '%s,%s,%s,%d,%d,%d,%d'%(x1_foreground_topic_num_pub,
    x2_foreground_topic_num_sub,
    x3_foreground_topic_rate,
    x4_colocated_topics,
    x5_background_num_pub,
    x6_background_num_sub,
    x7_background_rate)


if __name__ == "__main__":
  parser= argparse.ArgumentParser(description='script for testing load generated by using debs dataset')
  parser.add_argument('-run_id',required=True)
  parser.add_argument('-region_cars',nargs='*',required=True)
  args=parser.parse_args()
  
  #list of topic configurations
  #topic configuration= topic_name,aggregate_rate,publisher_distribution,subscriber_distribution,number_of_publishers,number_of_subscribers,total_endpoints,payload
  #publisher_distribution= topic_name:number_of_publishers:rate_of_publication:sample_count:payload 
  #subscriber_distribution= topic_name:number_of_subscribers:sample_count
  log_dir='/home/kharesp/log'
  run_id=args.run_id
  liveliness=2*60 #length of time (seconds) for which test should run
  config=[] 
  for region_car in args.region_cars:
    #region_car= r1:30
    region=int(region_car.split(':')[0][1:])
    cars=int(region_car.split(':')[1])
  
    #if (region==1):
    #  sample_count=liveliness
    #  #insert topic configuration for result topic to which all cars subscribe
    #  config.append('t%d,1,t%d:1:1:%d:1000,t%d:%d:%d,1,%d,%d,%d'%\
    #    (region,region,sample_count,region,cars,sample_count,cars,cars+1,1000))
    #else:
    #  sample_count=-1
    #  #insert topic configuration for result topic to which all cars subscribe
    #  config.append('r%d,1,r%d:1:1:%d:1000,r%d:%d:%d,1,%d,%d,%d'%\
    #    (region,region,sample_count,region,cars,sample_count,cars,cars+1,1000))

    #insert topic configuration for result topic to which all cars subscribe
    sample_count=liveliness
    config.append('t%d,1,t%d:1:1:%d:4000,t%d:%d:%d,1,%d,%d,%d'%\
      (region,region,sample_count,region,cars,sample_count,cars,cars+1,1000))

    #insert topic configuration for gps topic to which all cars send their gps update
    config.append('b%d,%d,b%d:%d:1:-1:4000,b%d:1:-1,%d,1,%d,%d'%\
      (region,cars,region,cars,region,cars,cars+1,100))

  if check(config):
    print('configuration is valid') 
    #extract subscriber configurations for all topics
    subscribers=['%s'%(tdesc.split(',')[3]) for tdesc in config]
    #extract publisher configurations for all topics
    publishers=['%s'%(tdesc.split(',')[2]) for tdesc in config]
    
    #place subscribers on available host machines
    subscriber_placement=place('sub',['%s'%(tdesc) for tdesc in subscribers if not tdesc.startswith('b')])
    #subscribers for gps topics are all hosted on node25
    subscriber_placement['node25']=['%s'%(tdesc) for tdesc in subscribers if tdesc.startswith('b')]
    #place publishers on available host machines
    publisher_placement=place('pub',['%s'%(tdesc) for tdesc in publishers])


    #partition foreground and background placement for subscribers
    foreground_background_sub= partition_foreground_background(subscriber_placement)
    #partition foreground and background placement for publishers
    foreground_background_pub= partition_foreground_background(publisher_placement)


    #clean-up before running any test
    print("\n\nCleaning logs directory")
    util.clean_logs(','.join(subscriber_placement.keys()+publisher_placement.keys()+brokers))

    #Restart edge-broker
    #print("\n\nRestarting EdgeBroker")
    #util.start_eb(brokers) 

    #start background publishers and subscribers
    print("\n\nStarting Background Endpoints")
    print("Background subscribers:{}".format(foreground_background_sub['background']))
    print("Background publishers:{}".format(foreground_background_pub['background']))
    background_load=util.Coordinate("load",run_id,foreground_background_sub['background'],\
      foreground_background_pub['background'])
    background_load.start()
    background_load.stop()

    #wait for sometime before starting experiment
    print('Will wait for sometime before starting test')
    time.sleep(20) 
   
    #start the experiment and wait for it to finish
    print("\n\nStarting Foreground Endpoints")
    print("Foreground subscribers:{}".format(foreground_background_sub['foreground']))
    print("Foreground publishers:{}".format(foreground_background_pub['foreground']))
    foreground_load=util.Coordinate("test",run_id,foreground_background_sub['foreground'],\
      foreground_background_pub['foreground'])
    util.start_monitors(run_id,"test",0,','.join(brokers))
    foreground_load.start()
    foreground_load.wait()
    foreground_load.stop()

    #collect logs
    print("\n\nCollecting Logs")
    client_and_broker_machines=[]
    map(lambda x: client_and_broker_machines.append(x),brokers) 
    map(lambda x: client_and_broker_machines.append(x),foreground_background_sub['foreground'].keys()) 
    util.collect_logs(run_id,log_dir,','.join(client_and_broker_machines))

    #copy model features 
    print("\n\nCopying model features")
    features=model_features(config)
    with open('%s/%s/features'%(log_dir,run_id),'w') as f:
      f.write('foreground_#pub,foreground_#sub,foreground_rate,#colocated_topics,background_#pub,background_#sub,background_rate\n')
      f.write(features+'\n')

    #write test configuration
    print("\n\nSaving test configuration")
    with open('%s/%s/config'%(log_dir,run_id),'w') as f:
      f.write('topic_name,aggregate_rate,publisher_distribution,subscriber_distribution,number_of_publishers,number_of_subscribers,total_endpoints,payload\n')
      for topic_description in config:
        f.write(topic_description+'\n') 

    #kill publishers and subscribers before exiting
    print("\n\nCleaning up endpoints before exiting")
    util.kill(pattern='edgent.endpoints',hosts=','.join(subscriber_placement.keys() +\
       publisher_placement.keys()))

    #summarize results
    print("\n\nSummarizing results")
    subprocess.check_call(['python','src/plots/summarize/summarize.py',\
      '-log_dir',log_dir,'-sub_dirs',str(run_id)])
   
  else:
    print('configuration cannot be tested')
