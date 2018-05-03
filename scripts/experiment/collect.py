import os
import numpy as np

k_colocation=6
runs=1
iterations=150
log_dir='/home/kharesp/log/colocated_with_proc_%d_test'%(k_colocation)
output_file_path='/home/kharesp/log/colocated_with_proc_%d_with_variance.csv'%(k_colocation)

def load_configuration(test_id):
  config=[]
  with open('%s/run1/%d/config'%(log_dir,test_id),'r') as f: 
    next(f) #skip header
    for line in f:
      config.append(line) 
  return config
  
def model_features(test_id):    
  config= load_configuration(test_id)

  features={}
  for tdesc in config: 
    parts= tdesc.split(',')
    topic_name= parts[0]
    processing_interval= parts[1]
    publication_rate= parts[2]
    background_sum_load=0 
    background_sum_interval=0
    background_sum_rate=0

    for other_tdesc in config: 
      other_topic_name= other_tdesc.split(',')[0]
      if not (other_topic_name == topic_name):
        background_sum_interval+=int(other_tdesc.split(',')[1])
        background_sum_rate+=int(other_tdesc.split(',')[2])
        background_sum_load+=float(other_tdesc.split(',')[-1])
  
    features[topic_name]={
      'interval': int(processing_interval),
      'rate': int(publication_rate),
      'background_sum_interval': background_sum_interval,
      'background_sum_rate': background_sum_rate,
      'background_sum_load': background_sum_load}

  return features

def model_output(test_id):
  toGb=.000008
  output={'cpu':[],
    'mem':[],
    'nw': [],}

  for runid in range(1,runs+1,1):
    with open('%s/run%d/%d/summary/summary_topic.csv'%\
      (log_dir,runid,test_id),'r') as f: 
      next(f) #skip header
      for line in f:
        parts=line.split(',')
        if parts[0] in output:
          output[parts[0]].append(float(parts[9]))
        else: 
          output[parts[0]]=[]
          output[parts[0]].append(float(parts[9]))

    with open('%s/run%d/%d/summary/summary_util.csv'%\
      (log_dir,runid,test_id),'r') as f: 
      next(f) #skip header
      hostname,cpu,iowait,mem,nw= next(f).split(',')
      output['cpu'].append(float(cpu))
      output['mem'].append(float(mem))
      output['nw'].append(float(nw)*toGb)

  return output

with open('%s'%(output_file_path),'w') as f:
  f.write('foreground_interval,foreground_rate,background_sum_load,background_sum_interval,background_sum_rate,mean_90th,std_90th,mean_cpu\n') 
  for test_id in range(1,iterations+1,1):
    #model features
    features= model_features(test_id)
    #model output
    output= model_output(test_id)

    for topic_name, x in features.items():
      f.write('%d,%d,%f,%d,%d,%f,%f,%f\n'%(x['interval'],
        x['rate'],
        x['background_sum_load'],
        x['background_sum_interval'],
        x['background_sum_rate'],
        np.mean(output[topic_name]),
        np.std(output[topic_name]),
        np.mean(output['cpu'])))
