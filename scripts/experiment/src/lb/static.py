import argparse,random,sys,os,itertools,time
from sklearn.externals import joblib
from sklearn.neural_network import MLPRegressor
import numpy as np
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/tests')

models_dir='/home/kharesp/learned_models/600_runs'
isolated_models_dir='/home/kharesp/learned_models'
intervals= [10,20,30,40]
deadline=1000
threshold=100

max_supported_rate={
  10: 78,
  20: 37,
  30: 24,
  40: 20,
}

#upto_threshold_requests
#range_of_rates={
#  10: np.arange(1,max_supported_rate[10]+1),
#  20: np.arange(1,max_supported_rate[20]+1),
#  30: np.arange(1,max_supported_rate[30]+1),
#  40: np.arange(1,max_supported_rate[40]+1), 
#}

#5_below_threshold_requests
range_of_rates={
  10: np.arange(1,max_supported_rate[10]-5),
  20: np.arange(1,max_supported_rate[20]-5),
  30: np.arange(1,max_supported_rate[30]-5),
  40: np.arange(1,max_supported_rate[40]-5), 
}

def create_skewed_request(topics):
  req=''
  for topic in range(1,topics+1,1):
    higher_rate= random.choice([True,False])  
    if higher_rate:
      #pick processing interval at random
      interval=intervals[random.randint(0,len(intervals)-1)]
      #pick publication rate at random
      rate=range_of_rates[interval][random.randint(len(range_of_rates[interval])-5,
        len(range_of_rates[interval])-1)]
      #add to req string
      req+='t%d:%d:%d,'%(topic,interval,rate)
    else:
      #pick processing interval at random
      interval=intervals[random.randint(0,len(intervals)-1)]
      #pick publication rate at random
      rate=range_of_rates[interval][random.randint(0,4)]
      #add to req string
      req+='t%d:%d:%d,'%(topic,interval,rate)
  return req.rstrip(',')


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

def create_request_file(topics,number_of_requests,file_path):
  with open(file_path,'w') as f:
    for i in range(number_of_requests):
      req=create_skewed_request(topics)
      f.write(req+'\n')

class StaticPlacement:
  def __init__(self,max_topics,k_dash):
    self.max_topics=max_topics
    self.k_dash=k_dash
    #load models
    self.load_models()

  def load_models(self):
    self.load_isolated_topic_model()
    self.scalers={} 
    self.models={}
    for topic in range(2,self.max_topics+1,1):
      self.scalers[topic]=joblib.load('%s/%d_colocation_scaler.pkl'%(models_dir,topic))
      self.models[topic]=joblib.load('%s/%d_colocation.pkl'%(models_dir,topic))

  def load_isolated_topic_model(self):
    self.isolated_topic_scaler=joblib.load('%s/isolated_topic_scaler.pkl'%(isolated_models_dir))
    self.isolated_topic_poly=joblib.load('%s/isolated_topic_poly.pkl'%(isolated_models_dir))
    self.isolated_topic_model=joblib.load('%s/isolated_topic_model.pkl'%(isolated_models_dir))

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
    if (len(existing_topics)>self.max_topics):
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

      X=[[f_p,f_r,bkg_load,bkg_sum_processing,bkg_sum_rate]]
      predicted_latency=np.exp(model.predict(scaler.transform(X)))
      if (predicted_latency[0] > (deadline - threshold)):
        return False
    return True


  def hybrid2(self,partitions,k_prime):
    broker_count=1
    sorted_topic_list=self.order_by_latency(partitions,descending=True)

    placement={}   
    while (len(sorted_topic_list) >= k_prime):
      feasible_k_prime_set_found=False
      #find a feasible set of size k_prime
      for combination in itertools.combinations(sorted_topic_list,k_prime):
        if self.check_feasibility(combination):
          feasible_k_prime_set_found=True
          placement['b%d'%(broker_count)]=list(combination)
          for topic in combination:
            sorted_topic_list.remove(topic)
          break
      if not feasible_k_prime_set_found:
        break

      #try to place remaining topics on this broker
      additional_topics=[]
      for topic in sorted_topic_list:
        p=placement['b%d'%(broker_count)]
        if (self.check_feasibility(p+[topic])):
          placement['b%d'%(broker_count)].append(topic)
          additional_topics.append(topic)

      for topic in additional_topics:
        sorted_topic_list.remove(topic)
      #increment broker_count
      broker_count+=1

    #resort to feasibility set with k_prime - 1

    while (len(sorted_topic_list)>0): #continue until all topics are placed 
      placed_on_current_broker=False
      for size in range(min(len(sorted_topic_list),k_prime-1),0,-1):
        for combination in itertools.combinations(sorted_topic_list,size):
          if self.check_feasibility(combination): 
            placement['b%d'%(broker_count)]=combination
            for topic in combination: 
              sorted_topic_list.remove(topic)
            broker_count+=1
            placed_on_current_broker=True
            break
        if placed_on_current_broker:
          break
    return placement
 

  def hybrid(self,partitions,K):
    broker_count=1
    placement={}
    topic_list=list(partitions)

    while (len(topic_list) > 0):
      sorted_topic_list=self.order_by_latency(topic_list,descending=True)
      placed=False
      #try to place a set of K or less topics on current broker
      for size in range(min(len(sorted_topic_list),K),0,-1):
        for combination in itertools.combinations(sorted_topic_list,size):
          if self.check_feasibility(combination):
            placement['b%d'%(broker_count)]=list(combination)
            for topic in combination:
              topic_list.remove(topic)
              sorted_topic_list.remove(topic)
            placed=True
            break
        if placed:
          break

      additional_topics=[]
      for topic in sorted_topic_list:
        p=placement['b%d'%(broker_count)]
        if (self.check_feasibility(p+[topic])):
          placement['b%d'%(broker_count)].append(topic)
          additional_topics.append(topic)

      for topic in additional_topics:
        topic_list.remove(topic)

      broker_count+=1
    return placement
      
  def feasibility_set(self,partitions):
    placement={}
    broker_count=1
    sorted_topic_list=self.order_by_latency(partitions,descending=True)
    
    while (len(sorted_topic_list)>0): #continue until all topics are placed 
      placed_on_current_broker=False
      #try to find the maximal set of topics that can be placed on the current broker
      for size in range(min(self.max_topics,len(sorted_topic_list)),0,-1):
        for combination in itertools.combinations(sorted_topic_list,size):
          if self.check_feasibility(combination): 
            placement['b%d'%(broker_count)]=combination
            for topic in combination: 
              sorted_topic_list.remove(topic)
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

  def first_fit_rate_ordering(self,partitions):
    sorted_by_rate=[]
    rate_description={} 
    for p in partitions:
      name,interval,rate= p.split(':')
      if int(rate) in rate_description:
        rate_description[int(rate)].append(p)
      else:
        rate_description[int(rate)]=[p]

    for rate in sorted(rate_description):
      sorted_by_rate= sorted_by_rate + rate_description[rate]

    return self.first_fit(sorted_by_rate)

  def order_by_latency(self,partitions,descending=False):
    sorted_by_latency=[]
    latency_description={}
    for partition in partitions:
      topic,interval,rate= partition.split(':')
      X=[[int(interval),int(rate)]]
      scaled_features= self.isolated_topic_scaler.transform(X)
      polynomial_features= self.isolated_topic_poly.transform(scaled_features)
      latency=np.exp(self.isolated_topic_model.predict(polynomial_features))[0][0]

      if latency in latency_description:
        latency_description[latency].append(partition)
      else:
        latency_description[latency]=[partition]

    for latency in sorted(latency_description,reverse=descending):
      sorted_by_latency= sorted_by_latency + latency_description[latency]

    return sorted_by_latency
  
  def first_fit_latency_ordering(self,partitions,descending=False):
    return self.first_fit(self.order_by_latency(partitions,descending))

  def place(self,req,algo):
    #partitions= self.create_partitions(req)
    partitions=req.split(',')

    if algo=='ffu':
      return self.first_fit(partitions)
    if algo=='ffr':
      return self.first_fit_rate_ordering(partitions)
    if algo=='ffl':
      return self.first_fit_latency_ordering(partitions)
    if algo=='ffd':
      return self.first_fit_latency_ordering(partitions,descending=True)
    elif algo=='fs':
      return self.feasibility_set(partitions)
    elif algo=='hyb':
      return self.hybrid2(partitions,self.k_dash)
    else:
      print('placement algorithm:%s not recognized'%(algo))
      return None

  def check_correctness_of_placement(self,placement):
    if (len(placement)==1):
      return True
    topic_placement=[set(v) for v in placement.values()]
    return not bool(set.intersection(*topic_placement))


if __name__ == "__main__":
  algorithms=['hyb']
  n=[10,20,30,40,50]
  max_topics=6 
  iterations=5
  k_dash_values=[1,2,3,4,5,6]

  for algo in algorithms:
    for topic in n:
      for k_dash in k_dash_values: 
        lb=StaticPlacement(max_topics,k_dash)
        with open('/home/kharesp/static_placement/requests/skewed/n_%d'%(topic),'r') as fi,\
          open('/home/kharesp/static_placement/placement/skewed/varying_k/k_%d/n_%d'%(k_dash,topic),'w') as fp,\
          open('/home/kharesp/static_placement/results/skewed/varying_k/k_%d/n_%d'%(k_dash,topic),'w') as fr:
          for idx,line in enumerate(fi):
            if (idx+1)>iterations:
              break
            print('Creating placement for algo:%s, n:%d, k:%d, iter:%d'%(algo,topic,k_dash,idx+1))

            start_time_milli= int(round(time.time()*1000))
            placement=lb.place(line.rstrip(),algo)
            end_time_milli= int(round(time.time()*1000))
 
            if (lb.check_correctness_of_placement(placement)):
              print('Placement is correct')

            #write placement
            placement_str=''
            for b in range(len(placement)):
              broker='b%d'%(b+1)
              topic_description_list=sorted(placement[broker])
              topics=[description.split(':')[0] for description in topic_description_list]
              placement_str= placement_str + '%s:%s;'%(broker,','.join(topics))
            fp.write(placement_str.rstrip(';')+'\n')

            #write results
            print('brokers:%d,time:%d\n'%(len(placement),(end_time_milli-start_time_milli)))
            fr.write('%d,%d\n'%(len(placement),(end_time_milli-start_time_milli)))
