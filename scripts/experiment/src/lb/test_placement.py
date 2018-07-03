import sys,os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import metadata,util
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/tests')
import restart,rate_processing_interval_combinations
from kazoo.client import KazooClient
from functools import reduce

broker_id_host_map={
  'b1': 'EB-30-10.20.30.2-0',
  'b2': 'EB-30-10.20.30.3-0',
  'b3': 'EB-30-10.20.30.4-0',
  'b4': 'EB-30-10.20.30.5-0',
  'b5': 'EB-30-10.20.30.6-0',
  'b6': 'EB-30-10.20.30.7-0',
  'b7': 'EB-30-10.20.30.8-0',
  'b8': 'EB-30-10.20.30.9-0',
  'b9': 'EB-30-10.20.30.10-0',
  'b10': 'EB-30-10.20.30.11-0',
}

class TestPlacement: 
  def __init__(self,placement,zk_connector,fe_address,log_dir,run_id):
    self.placement=placement
    self.zk_connector=zk_connector
    self.fe_address=fe_address
    self.log_dir=log_dir 
    self.run_id=run_id

  def place_topics_on_brokers(self):
    self.zk= KazooClient(hosts=self.zk_connector)
    self.zk.start()
    for broker_id,topics in self.placement.items():
      eb_id=broker_id_host_map[broker_id]
      try:
        for t in topics:
          name,interval,rate= t.split(':')
          #create /topics/t
          self.zk.retry(self.zk.create,'/topics/%s'%(name),\
            bytes('%s,none'%(interval),'utf-8'))
          #create /lb/topics/t
          self.zk.retry(self.zk.ensure_path,'/lb/topics/%s'%(name))
          #create /eb/eb_id/t
          self.zk.retry(self.zk.ensure_path,'/eb/%s/%s'%(eb_id,name))
      except (KazooException,RetryFailedError) as e:
        print('Caught KazooException')
    self.zk.stop()

  def run(self):
    #restart brokers
    util.start_eb(','.join(rate_processing_interval_combinations.brokers),self.zk_connector)
    #place topics on brokers as per placement 
    self.place_topics_on_brokers()
    #start test
    topic_descriptions_list=reduce(lambda x,y:x+y,self.placement.values()) 
    config=rate_processing_interval_combinations.\
      create_configuration(','.join(topic_descriptions_list))
    restart.Hawk(config,self.log_dir,self.run_id,self.zk_connector,self.fe_address).run()

if __name__=="__main__":
  topic_counts=[30,40,50]
  algo='mpc'

  for t in topic_counts: 
    config_file='/home/kharesp/static_placement/requests/5_below_threshold/n_%d'%(t)
    placement_file='/home/kharesp/static_placement/placement/5_below_threshold/varying_n/%s/n_%d'%(algo,t)
    iterations=5

    zk_connector=metadata.public_zk
    fe_address='10.20.30.1'
    log_dir='/home/kharesp/static_placement/runtime/5_below_threshold/varying_n/%s/n_%d'%(algo,t)
    if not os.path.exists(log_dir):
      os.makedirs(log_dir)

    with open(placement_file,'r') as pf, open(config_file,'r') as cf:
      for idx in range(iterations):
        placement={}
        topic_descriptions={}
        placement_str=next(pf).rstrip()
        topic_description_str=next(cf).rstrip()
       
        for desc in topic_description_str.split(','):
          topic_name=desc.partition(':')[0] 
          topic_descriptions[topic_name]=desc

        for p in placement_str.split(';'):
          broker, sep, topics= p.partition(':')
          placement[broker]=[topic_descriptions[t] for t in topics.split(',')]

        #test placement 
        print('\n\n\nStarting test for n:%d, idx:%d'%(t,idx+1)) 
        TestPlacement(placement,zk_connector,fe_address,log_dir,idx+1).run()
