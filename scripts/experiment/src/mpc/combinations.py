import numpy as np
from sklearn.externals import joblib
import time

#constants
isolated_models_dir='/home/kharesp/learned_models'
models_dir='/home/kharesp/learned_models/600_runs'
deadline=1000
threshold=100

class StaticPlacement:
  def __init__(self,max_topics,max_brokers):
    self.max_topics=max_topics
    self.max_brokers=max_brokers
    self.load_isolated_topic_model()
    self.load_models()  

  def load_models(self):
    self.scalers={}
    self.models={}
    for topic in range(2,self.max_topics+1,1):
      self.scalers[topic]=joblib.load('%s/%d_colocation_scaler.pkl'%(models_dir,topic))
      self.models[topic]=joblib.load('%s/%d_colocation.pkl'%(models_dir,topic))

  def load_isolated_topic_model(self):
    self.isolated_topic_scaler=joblib.load('%s/isolated_topic_scaler.pkl'%(isolated_models_dir))
    self.isolated_topic_poly=joblib.load('%s/isolated_topic_poly.pkl'%(isolated_models_dir))
    self.isolated_topic_model=joblib.load('%s/isolated_topic_model.pkl'%(isolated_models_dir))

  def combinations(self,placement,topic):
    result=[]
    for b in range(self.max_brokers):
      current_placement=placement.copy()
      open_positions= np.argwhere(np.isnan(current_placement[:,b]))
      if np.size(open_positions,0)==0:
        continue
      if np.size(open_positions,0)==self.max_topics:
        current_placement[0,b]=topic
        result.append(current_placement.copy())
        break
      current_placement[open_positions[0],b]=topic
      result.append(current_placement.copy())
    return result 

  def k_colocation_feasibility(self,existing_topics):
    if len(existing_topics)==1:
      return True
    
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

  def check_feasibility(self,placement):
    for b in range(self.max_brokers):
      topics_on_broker=placement[:,b]
      #filter out nan
      topics_on_broker=topics_on_broker[~np.isnan(topics_on_broker)]
      topic_list=['t%d:%d:%d'%(t,self.topic_intervals['t%d'%(t)],
        self.topic_rates['t%d'%(t)]) for t in topics_on_broker]
      if len(topic_list)>0:
        if not self.k_colocation_feasibility(topic_list):
          return False
    return True

  def number_of_brokers(self,placement): 
    number_of_brokers=0
    for b in range(self.max_brokers):
      topics_on_broker=placement[:,b]
      #filter out nan
      topics_on_broker=topics_on_broker[~np.isnan(topics_on_broker)]
      if len(topics_on_broker) > 0:
        number_of_brokers+=1
    return number_of_brokers
 
  def latencies(self,existing_topics): 
    l=[]
    if len(existing_topics)==1:
      name,interval,rate= existing_topics[0].split(':')
      X=[[int(interval),int(rate)]]
      scaled_features= self.isolated_topic_scaler.transform(X)
      polynomial_features= self.isolated_topic_poly.transform(scaled_features)
      predicted_latency=np.exp(self.isolated_topic_model.predict(polynomial_features))[0][0]
      l.append(predicted_latency)
    else: 
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
        l.append(predicted_latency)
    while len(l) < self.max_topics:
      l.append(0)
    return l

  def latency_matrix(self,placement):
    l= np.zeros((self.max_topics,self.max_brokers))
    for b in range(self.max_brokers):
      topics_on_broker=placement[:,b]
      #filter out nan
      topics_on_broker= topics_on_broker[~np.isnan(topics_on_broker)]
      if len(topics_on_broker)>0:
        topic_list= ['t%d:%d:%d'%(t,self.topic_intervals['t%d'%(t)],
          self.topic_rates['t%d'%(t)]) for t in topics_on_broker]
        latency_list= self.latencies(topic_list)
        l[:,b]=latency_list
    return l

  def average_latency(self,placement):
    return np.mean(self.latency_matrix(placement))

  def predictions(self,placement):
    topic_predictions={}
    lm= self.latency_matrix(placement)
    for b in range(self.max_brokers):
      for t in range(self.max_topics):  
        topic= placement[t,b]
        latency= lm[t,b]
        if not np.isnan(topic):
          topic_predictions['t%d'%(topic)]=latency
    predictions_str=''
    for i in range(len(topic_predictions)): 
      predictions_str+='t%d:%f,'%(i+1,topic_predictions['t%d'%(i+1)])
    return predictions_str.rstrip(',')
 
  def placement_string(self,placement):
    placement_str='' 
    for b in range(self.max_brokers):
      topics_on_broker= placement[:,b] 
      #filter out nan
      topics_on_broker= topics_on_broker[~np.isnan(topics_on_broker)]
      if (len(topics_on_broker)>0):
        topic_names_on_broker=['t%d'%(v) for v in topics_on_broker]
        placement_str+='b%d:%s;'%(b+1,','.join(topic_names_on_broker))
      
    return placement_str.rstrip(';')

  def place(self,topic_list):
    self.topic_intervals={}
    self.topic_rates={}
    for tdesc in topic_list:
      name,interval,rate= tdesc.split(':')
      self.topic_intervals[name]=int(interval)
      self.topic_rates[name]=int(rate)

    self.placement=np.full((self.max_topics,self.max_brokers),np.nan)

    for t in [int(desc.split(':')[0][1:]) for desc in topic_list]:
      alternatives=self.combinations(self.placement,t)
      print('\nPossible alternatives are:')
      for alt in alternatives:
        print(alt)
      feasible_alternatives= [a for a in alternatives if self.check_feasibility(a)]
      #get placement with minimum number of brokers
      number_of_brokers=[self.number_of_brokers(p) for p in feasible_alternatives]
      self.placement=feasible_alternatives[number_of_brokers.index(min(number_of_brokers))]
      print('Chosen alternative:')
      print(self.placement)

      #get placement with minimum overall average latency 
      #placement_latencies=[self.average_latency(p) for p in feasible_alternatives]
      #self.placement=feasible_alternatives[placement_latencies.index(min(placement_latencies))]
    return self.placement

if __name__=="__main__":
  tests=1
  n=10
  s= StaticPlacement(6,5)
  with open('/home/kharesp/static_placement/requests/5_below_threshold/n_%d'%(n),'r') as f:
    #open('/home/kharesp/static_placement/placement/5_below_threshold/varying_n/mpc/n_%d'%(n),'w') as placement_f,\
    #open('/home/kharesp/static_placement/results/5_below_threshold/varying_n/mpc/n_%d'%(n),'w') as results_f,\
    #open('/home/kharesp/static_placement/placement/5_below_threshold/varying_n/mpc/prediction_%d'%(n),'w') as predictions_f:
    for idx,line in enumerate(f):
      if (idx+1) > tests:
        break
      start_time_milli= int(round(time.time()*1000))
      placement= s.place(line.rstrip().split(','))
      end_time_milli= int(round(time.time()*1000))
      print(placement)
      
      #time_to_find_placement= end_time_milli - start_time_milli

      #placement_str= s.placement_string(placement)
      #number_of_brokers= s.number_of_brokers(placement)
      #predictions_str= s.predictions(placement)

      #placement_f.write(placement_str+'\n')
      #predictions_f.write(predictions_str+'\n')
      #results_f.write('%d,%d\n'%(number_of_brokers,time_to_find_placement))
