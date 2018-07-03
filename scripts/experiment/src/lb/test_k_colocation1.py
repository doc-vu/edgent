import sys,os,random
import numpy as np
from sklearn.externals import joblib
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import metadata,util
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/tests')
import restart,rate_processing_interval_combinations

intervals= [10,20,30,40]
rates=10
deadline=1000
threshold=100

max_supported_rate={
  10: 78,
  20: 37,
  30: 24,
  40: 20,
}

#range_of_rates={ #msg/sec
#  10:[int(v) for v in np.linspace(1,max_supported_rate[10],rates)],
#  20:[int(v) for v in np.linspace(1,max_supported_rate[20],rates)],
#  30:[int(v) for v in np.linspace(1,max_supported_rate[30],rates)],
#  40:[int(v) for v in np.linspace(1,max_supported_rate[40],rates)],
#}

range_of_rates={ #msg/sec
  10: np.arange(1,max_supported_rate[10]+1),
  20: np.arange(1,max_supported_rate[20]+1),
  30: np.arange(1,max_supported_rate[30]+1),
  40: np.arange(1,max_supported_rate[40]+1),
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

def predict_latency(request,models_dir):
  results={}
  topics=request.split(',')
  scaler=joblib.load('%s/%d_colocation_scaler.pkl'%(models_dir,len(topics)))
  model=joblib.load('%s/%d_colocation.pkl'%(models_dir,len(topics)))
  #model_variance=joblib.load('%s/%d_colocation_variance.pkl'%(models_dir,len(topics)))

  for current_topic in topics:
    curr_name,curr_interval,curr_rate= current_topic.split(':')
    f_p= int(curr_interval)
    f_r= int(curr_rate)
    bkg_sum_load=0
    bkg_sum_processing=0
    bkg_sum_rate=0

    for background_topic in topics:
      bkg_name,bkg_interval,bkg_rate= background_topic.split(':')
      if (curr_name == bkg_name):
        continue
      bkg_sum_load+=int(bkg_interval) * int(bkg_rate)/1000.0
      bkg_sum_rate+=int(bkg_rate)
      bkg_sum_processing+=int(bkg_interval)

    X=[[f_p,f_r,bkg_sum_load,bkg_sum_processing,bkg_sum_rate]]
    predicted_latency=np.exp(model.predict(scaler.transform(X)))
    #predicted_std=np.exp(model_variance.predict(scaler.transform(X)))
    predicted_std=0
    results[curr_name]={ '90th': predicted_latency,
      'std': predicted_std,
      'X': str(X)}

  return results

def run(run_id,topics,models_dir,log_dir,req=None):
  if not req:
    #create request
    req= create_request(topics)
  #get predicted latency values
  predicted_latencies= predict_latency(req,models_dir)
  print(predicted_latencies)

  #restart brokers
  print('Restarting EdgeBroker') 
  util.start_eb(','.join(rate_processing_interval_combinations.brokers),zk_connector)

  #run test
  print('Starting test') 
  config=rate_processing_interval_combinations.\
        create_configuration(req)
  restart.Hawk(config,log_dir,run_id,zk_connector,fe_address).run()

  #write prediction results
  with open('%s/%d/prediction'%(log_dir,run_id),'w') as f:
    for topic,prediction in predicted_latencies.items():
      if ((prediction['90th'] + prediction['std']) > (deadline-threshold)):
        f.write('%s,%s,%f,%f,False\n'%(topic,prediction['X'],prediction['90th'],prediction['std']))
      else:
        f.write('%s,%s,%f,%f,True\n'%(topic,prediction['X'],prediction['90th'],prediction['std']))

def create_request_file(topics,count,path):
  with open(path,'w') as f: 
    for iteration in range(1,count+1,1):
      req=create_request(topics)
      f.write(req+'\n')
    
  
if __name__=="__main__":
  #constants
  zk_connector=metadata.public_zk
  fe_address='10.20.30.1'
  topics=6
  experiment_runs=600
  test_iterations=2
  
  models_dir='/home/kharesp/learned_models/%d_runs'%(experiment_runs)
  base_log_dir='/home/kharesp/rate_vs_interval_impact_test'
  if not os.path.exists(base_log_dir):
    os.makedirs(base_log_dir)

  #check if requests file exists
  #if not os.path.exists('%s/upto_threshold_requests'%(base_log_dir)):
  #  create_request_file(topics,test_iterations,'%s/upto_threshold_requests'%(base_log_dir))

  log_dir='%s'%(base_log_dir)
  if not os.path.exists(log_dir):
    os.makedirs(log_dir)

  with open('%s/requests'%(base_log_dir),'r') as f: 
   for idx,line in enumerate(f):
      print(line) 
      run(idx+1,topics,models_dir,log_dir,req=line)
