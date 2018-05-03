import os

def load_configuration(config_path):
  config=[]
  with open(config_path,'r') as f:
    #skip header
    next(f)
    for line in f:
      config.append(line.rstrip())
  return config

def model_features(config):
  res={}
  for tdesc in config:
    parts= tdesc.split(',')
    topic_name=parts[0]
    processing_interval=parts[1]
    publication_rate=parts[2]
    background_rate_x_processing_interval=0
    for tdesc in config:
      other_topic_name=tdesc.split(',')[0]
      if not other_topic_name==topic_name:
        background_rate_x_processing_interval+= float(tdesc.split(',')[-1])
        
    res[topic_name]='%s,%s,%f'%(processing_interval,publication_rate,background_rate_x_processing_interval)
  return res
  
def model_output(log_dir,run_id):
  toGb=.000008
  res={}
  with open('%s/%s/summary/summary_util.csv'%(log_dir,run_id),'r') as f:
    #skip header:hostname,avg_cpu(%),avg_iowait(%),avg_mem(gb),avg_nw(kB/sec)
    next(f)
    hostname,cpu,iowait,mem,nw= next(f).split(',')
    res['cpu']=float(cpu)
    res['mem']=float(mem)
    res['nw']=float(nw)*toGb

  res['90th']={}
  res['avg']={}
  with open('%s/%s/summary/summary_topic.csv'%(log_dir,run_id),'r') as f:
    #skip header:
    next(f)
    for line in f:
      parts=line.split(',')
      res['90th'][parts[0]]=float(parts[9])
      res['avg'][parts[0]]=float(parts[2])
  return res


log_dir='/home/kharesp/log'
topics=[8,9,10,11,12,13,14,15,16,17]
for count in topics:
  sub_dirs=os.listdir('%s/colocated_with_proc_%d'%(log_dir,count))
  sorted_dirs=sorted([int(d) for d in sub_dirs])
  for iteration in sorted_dirs:
    config=load_configuration('%s/colocated_with_proc_%d/%d/config'%(log_dir,count,iteration))
    features=model_features(config)
    y=model_output('%s/colocated_with_proc_%d'%(log_dir,count),iteration)

    #copy model features 
    with open('%s/colocated_with_proc_%d/%d/features'%(log_dir,count,iteration),'w') as f:
      f.write('test_id,#background_topics,foreground_processing_interval,foreground_rate,background_rate_x_processing_interval,\
        foreground_avg_latency,foreground_90th_percentile_latency,broker_cpu,broker_mem,broker_nw\n')
      for topic,features in features.items():
        f.write('%d,%d,%s,%f,%f,%f,%f,%f\n'%(iteration,count-1,features,y['avg'][topic],y['90th'][topic],y['cpu'],y['mem'],y['nw']))
