import os
import numpy as np

deadline=1000
k_colocation=5
runs=1
log_dir='/home/kharesp/workspace/java/edgent/model_learning/data/%d_colocation'%(k_colocation)
output_file_path='/home/kharesp/workspace/java/edgent/model_learning/data/%d_colocation/%d_colocation_wo_variance_non_aggregate.csv'%(k_colocation,k_colocation)

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
  #output={'cpu':[],
  #  'mem':[],
  #  'nw': [],}
  output={}

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

    #with open('%s/run%d/%d/summary/summary_util.csv'%\
    #  (log_dir,runid,test_id),'r') as f: 
    #  next(f) #skip header
    #  hostname,cpu,iowait,mem,nw= next(f).split(',')
    #  output['cpu'].append(float(cpu))
    #  output['mem'].append(float(mem))
    #  output['nw'].append(float(nw)*toGb)

  return output

###aggregate
#with open('%s'%(output_file_path),'w') as f:
#  f.write('test_id,foreground_interval,foreground_rate,background_sum_load,background_sum_interval,background_sum_rate,mean_90th,mask\n') 
#  iterations=[int(v) for v in os.listdir('%s/run1'%(log_dir))]
#  for test_id in sorted(iterations):
#    #model features
#    features= model_features(test_id)
#    #model output
#    output= model_output(test_id)
#
#    for topic_name, x in features.items():
#      f.write('%d,%d,%d,%f,%d,%d,%f,%d\n'%(test_id,x['interval'],
#        x['rate'],
#        x['background_sum_load'],
#        x['background_sum_interval'],
#        x['background_sum_rate'],
#        np.mean(output[topic_name]),
#        (1 if np.mean(output[topic_name])>deadline else 0)))
#        #np.std(output[topic_name])))
#        #np.mean(output['cpu'])))

###non_aggregate_vec
#with open('%s'%(output_file_path),'w') as f:
#  f.write('test_id,#topics,processing_intervals,publication_rates,latency_90th,mask\n') 
#  iterations=[int(v) for v in os.listdir('%s/run1'%(log_dir))]
#  for test_id in sorted(iterations):
#    #model features
#    features= model_features(test_id)
#    #model output
#    output= model_output(test_id)
#
#    processing_intervals_str=''
#    publication_rates_str=''
#    latency_str=''
#    mask_str=''
#
#    for i in range(k_colocation):
#      processing_intervals_str+='%d,'%(features['t%d'%(i+1)]['interval'])
#      publication_rates_str+='%d,'%(features['t%d'%(i+1)]['rate'])
#      latency_str+='%f,'%(np.mean(output['t%d'%(i+1)]))
#      mask_str+='%d,'%(1 if (np.mean(output['t%d'%(i+1)])>deadline) else 0)
#    f.write('%d;%d;%s;%s;%s;%s\n'%(test_id,k_colocation,
#      processing_intervals_str.rstrip(','),
#      publication_rates_str.rstrip(','),
#      latency_str.rstrip(','),
#      mask_str.rstrip(',')))

##non_aggregate
with open('%s'%(output_file_path),'w') as f:
  f.write('test_id,#topics,processing_intervals,publication_rates,latency_90th,mask\n') 
  iterations=[int(v) for v in os.listdir('%s/run1'%(log_dir))]
  for test_id in sorted(iterations):
    #model features
    features= model_features(test_id)
    #model output
    output= model_output(test_id)

    for curr_topic in range(k_colocation):
      processing_intervals_str='%d,'%(features['t%d'%(curr_topic+1)]['interval'])
      publication_rates_str='%d,'%(features['t%d'%(curr_topic+1)]['rate'])
      for topic in range(k_colocation):
        if (curr_topic==topic):  
          continue
        processing_intervals_str+='%d,'%(features['t%d'%(topic+1)]['interval'])
        publication_rates_str+='%d,'%(features['t%d'%(topic+1)]['rate'])

      f.write('%d;%d;%s;%s;%f;%d\n'%(test_id,k_colocation,
        processing_intervals_str.rstrip(','),
        publication_rates_str.rstrip(','),
        np.mean(output['t%d'%(curr_topic+1)]),
        1 if (np.mean(output['t%d'%(curr_topic+1)])>deadline) else 0))
