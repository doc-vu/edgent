import argparse,random,sys,os,itertools,time
from sklearn.externals import joblib
from sklearn.neural_network import MLPRegressor
import numpy as np
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import util
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/tests')
import restart,rate_processing_interval_combinations
from functools import reduce
from kazoo.client import KazooClient
from kazoo.retry import RetryFailedError
from kazoo.exceptions import KazooException

broker_id_host_map={
  'b1': 'EB-30-10.20.30.2-0',
}
models_dir='/home/kharesp/log/models'
intervals= [10,20,30,40]
rates=10
max_topics=6
deadline=1000
threshold=100

max_supported_rate={
  10: 78,
  20: 37,
  30: 24,
  40: 20,
}


range_of_rates={ #msg/sec
  10:[int(v) for v in np.linspace(max_supported_rate[10]-5,max_supported_rate[10],5)],
  20:[int(v) for v in np.linspace(max_supported_rate[20]-5,max_supported_rate[20],5)],
  30:[int(v) for v in np.linspace(max_supported_rate[30]-5,max_supported_rate[30],5)],
  40:[int(v) for v in np.linspace(max_supported_rate[40]-5,max_supported_rate[40],5)],
}

def create_request(topics):
  req=''
  for topic in range(1,topics+1,1):
    #pick processing interval at random
    interval=intervals[random.randint(0,len(intervals)-1)]
    #pick publication rate at random
    rate=range_of_rates[interval][random.randint(0,len(range_of_rates[interval])-1)]
    #add to req string
    req+='t%d:%d:%d,'%(topic,interval,rate)
  return req.rstrip(',')


class StaticPlacement:
  def __init__(self,zk_connector,fe_address,log_dir,run_id):
    self.zk_connector=zk_connector
    self.fe_address=fe_address
    self.log_dir=log_dir
    self.run_id=run_id
    #load models
    self.load_models()

  def load_models(self):
    self.scalers={} 
    self.models={}
    for topic in range(2,max_topics+1,1):
      self.scalers[topic]=joblib.load('%s/%d_colocation_scaler.pkl'%(models_dir,topic))
      self.models[topic]=joblib.load('%s/%d_colocation.pkl'%(models_dir,topic))

  def create_partitions(self,req):
    topic_configurations=req.split(',')
    partitions=[]
    for config in topic_configurations:
      name,interval,rate=config.split(':')
      num_partitions=int(rate)//max_supported_rate[int(interval)]
      remaining_load=int(rate)%max_supported_rate[int(interval)]
      for x in range(1,num_partitions+1,1): 
        partitions.append('%sp%d:%d:%d'%\
        (name,x,int(interval),max_supported_rate[int(interval)]))

      if (remaining_load>0):
        partitions.append('%sp%d:%d:%d'%\
          (name,num_partitions+1,int(interval),int(remaining_load)))

    return partitions

  def check_feasibility(self,existing_topics):
    if (len(existing_topics)==1):
      return True
    if (len(existing_topics)>max_topics):
      return False

    scaler=self.scalers[len(existing_topics)]
    model=self.models[len(existing_topics)]

    for current_topic in existing_topics:
      curr_name,curr_interval,curr_rate= current_topic.split(':')
      f_p= int(curr_interval)
      f_r= int(curr_rate)
      bkg_load=0
      bkg_sum_rate=0
      bkg_sum_processing=0

      for background_topic in existing_topics:
        bkg_name,bkg_interval,bkg_rate= background_topic.split(':')
        if (curr_name == bkg_name):
          continue
        bkg_load+=int(bkg_interval) * int(bkg_rate)/1000.0
        bkg_sum_rate+=int(bkg_rate)
        bkg_sum_processing+=int(bkg_interval)

      X=[[f_p,f_r,bkg_load,bkg_sum_rate,bkg_sum_processing]]
      predicted_latency=np.exp(model.predict(scaler.transform(X)))
      if (predicted_latency > (deadline - threshold)):
        return False
    return True
 
  def feasibility_set(self,partitions):
    placement={}
    broker_count=1
    topic_set=set(partitions)
    
    while (len(topic_set)>0): #continue until all topics are placed 
      #print('\nWill attempt to place: %d topics'%(len(topic_set)))
      placed_on_current_broker=False
      #try to find the maximal set of topics that can be placed on the current broker
      for size in range(min(max_topics,len(topic_set)),0,-1):
        #print('will try to find a feasible set of size:%d'%(size))
        for combination in itertools.combinations(topic_set,size):
          if self.check_feasibility(combination): 
            placement['b%d'%(broker_count)]=combination
            for topic in combination: 
              topic_set.remove(topic)
            broker_count+=1
            placed_on_current_broker=True
            break
        if placed_on_current_broker:
          break
    return placement
     
  def first_fit(self,partitions): 
    placement={'b1':[]}
    for topic_partition in partitions:
      placed=False
      #check if this topic partition can be placed on any existing broker
      for broker in sorted(placement.keys()): 
        p=placement[broker]
        if (self.check_feasibility(p+[topic_partition])):
          placement[broker].append(topic_partition)
          placed=True
          break
      if not placed:
        #spawn new broker
        placement['b%d'%(len(placement)+1)]=[topic_partition]
    return placement

  def place(self,req,algo):
    #partitions= self.create_partitions(req)
    partitions=req.split(',')

    if algo=='first_fit':
      return self.first_fit(partitions)
    elif algo=='feasibility_set':
      return self.feasibility_set(partitions)
    else:
      print('placement algorithm:%s not recognized'%(algo))
      return None

  def check_correctness_of_placement(self,placement):
    #if (len(placement) > 1): #currently only check for one broker placement
    #  return False
    if (len(placement)==1):
      return True
    topic_placement=[set(v) for v in placement.values()]
    return not bool(set.intersection(*topic_placement))
    
  def place_topics_on_brokers(self,placement):
    self.zk= KazooClient(hosts=self.zk_connector)
    self.zk.start()
    for broker_id,topics in placement.items():
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

  def run(self,req,algo):
    placement=self.place(req,algo)
    print(placement)

    if (self.check_correctness_of_placement(placement)):
      #restart brokers
      util.start_eb(','.join(rate_processing_interval_combinations.brokers),self.zk_connector)
      #place topics on brokers as per placement 
      self.place_topics_on_brokers(placement)
      #start test
      topic_descriptions_list=reduce(lambda x,y:x+y,placement.values()) 
      config=rate_processing_interval_combinations.\
        create_configuration(','.join(topic_descriptions_list))
      restart.Hawk(config,self.log_dir,self.run_id,self.zk_connector,self.fe_address).run()
    else:
      print('Placement for request:%s is invalid'%(req)) 


if __name__ == "__main__":
  #parser= argparse.ArgumentParser(description='script for static topic placement')
  #parser.add_argument('-topics',type=int,required=True)
  #parser.add_argument('-zk_connector',required=True)
  #parser.add_argument('-fe_address',required=True)
  #parser.add_argument('-log_dir',required=True)
  #parser.add_argument('-run_id',type=int,required=True)
  #args= parser.parse_args()

  #req=create_request(args.topics)
  req=create_request(100)
  #print('Packing topics for request:%s'%(req))

  #lb=StaticPlacement(args.zk_connector,args.fe_address,args.log_dir,args.run_id)
  lb=StaticPlacement('zk','fe','log_dir',1)
  #lb.run(req)

  start=time.time()
  placement_1=lb.place(req,'first_fit')
  end=time.time()
  print(end-start)
  if lb.check_correctness_of_placement(placement_1):
    #print(placement_1) 
    print(len(placement_1))

  start=time.time()
  placement_2=lb.place(req,'feasibility_set')
  end=time.time()
  print(end-start)
  if lb.check_correctness_of_placement(placement_2):
    #print(placement_2) 
    print(len(placement_2))
