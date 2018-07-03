import os

log_dir='/home/kharesp/k_colocation_validation/2_colocation/600_runs/5_below_threshold'
sub_dirs=[d for d in os.listdir(log_dir) if os.path.isdir(os.path.join(log_dir,d))]
sorted_dirs=sorted([int(v) for v in sub_dirs])

with open('%s/validation_data.csv'%(log_dir),'w') as output: 
  output.write('foreground_interval,foreground_rate,background_sum_load,background_sum_interval,background_sum_rate,90th_percentile\n')
  for d in sorted_dirs: 
    predicted_output={}
    with open('%s/%d/prediction'%(log_dir,d),'r') as f: 
      for line in f:
        topic_name=line.partition(',')[0]
        X=line[line.find('[[')+2:line.find(']]')]
        predicted_output[topic_name]={
          'X':X,
        }
    ground_truth={}
    with open('%s/%d/summary/summary_topic.csv'%(log_dir,d),'r') as f: 
      next(f)#skip header
      for line in f:
        parts=line.split(',')
        ground_truth[parts[0]]=parts[9]
  
    for topic,prediction in predicted_output.items():
      output.write('%s,%s\n'%(prediction['X'],ground_truth[topic]))
