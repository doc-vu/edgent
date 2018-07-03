from sklearn.externals import joblib
import numpy as np

isolated_models_dir='/home/kharesp/learned_models'
models_dir='/home/kharesp/learned_models/600_runs'

isolated_topic_scaler=None
isolated_topic_poly=None
isolated_topic_model=None
models={}
scalers={}
max_topics=6

def load_models():
  global isolated_topic_scaler
  isolated_topic_scaler=joblib.load('%s/isolated_topic_scaler.pkl'%(isolated_models_dir))
  global isolated_topic_poly
  isolated_topic_poly=joblib.load('%s/isolated_topic_poly.pkl'%(isolated_models_dir))
  global isolated_topic_model
  isolated_topic_model=joblib.load('%s/isolated_topic_model.pkl'%(isolated_models_dir))
  for k in range(2,max_topics+1,1):
    scalers[k]=joblib.load('%s/%d_colocation_scaler.pkl'%(models_dir,k))
    models[k]=joblib.load('%s/%d_colocation.pkl'%(models_dir,k))

def get_topic_configuration(topics,config_str):
  res=[]
  descriptions=config_str.split(',')
  for description in descriptions:
    topic_name,interval,rate= description.split(':')
    if topic_name in topics:
      res.append(description)
  return res

def get_placement(placement_string): 
  placement={}
  broker_topics= placement_string.split(';')
  for mapping in broker_topics:
    broker,sep,topics= mapping.partition(':')
    placement[broker]= topics.split(',')
  return placement

def get_isolated_topic_prediction(topic_description):
  topic,interval,rate= topic_description.split(':')
  X=[[int(interval),int(rate)]]
  scaled_features= isolated_topic_scaler.transform(X)
  polynomial_features= isolated_topic_poly.transform(scaled_features)
  latency=np.exp(isolated_topic_model.predict(polynomial_features))[0][0]
  return {topic:latency}

def get_predictions(existing_topics):
  if (len(existing_topics)==1):
    return get_isolated_topic_prediction(existing_topics[0])

  res={}
  scaler=scalers[len(existing_topics)]
  model=models[len(existing_topics)]

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
    res[curr_name]=predicted_latency
  return res

#constants
topic_counts=[10,20,30,40,50]
algorithms=['ffd','fs','hyb']
iterations=5


load_models()
for algo in algorithms:
  for n in topic_counts:
    requests_file='/home/kharesp/static_placement/requests/skewed/n_%d'%(n)
    placement_file='/home/kharesp/static_placement/placement/skewed/varying_n/%s/n_%d'%(algo,n)
    predictions_file='/home/kharesp/static_placement/placement/skewed/varying_n/%s/prediction_%d'%(algo,n)
    
    with open(requests_file,'r') as rf, open(placement_file,'r') as pf, open(predictions_file,'w') as lf:
      for idx in range(iterations):
        request_str=next(rf).rstrip()
        placement_str=next(pf).rstrip()
        placement=get_placement(placement_str)
        predictions={} 
        for broker,topics in placement.items():
          topic_configurations=get_topic_configuration(topics,request_str)
          res=get_predictions(topic_configurations)
          predictions.update(res)
    
        res_str=''
        for i in range(n):
          res_str=res_str+'t%d:%f,'%(i+1,predictions['t%d'%(i+1)])
        lf.write(res_str.rstrip(',')+'\n')
