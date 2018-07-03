import numpy as np
algorithms=['hyb']
k=[1,2,3,4,5,6]
iterations=5
deadline=1000

def number_of_topics_with_missed_deadline(path):
  missed_count=0
  with open(path,'r') as f:
    next(f) #skip header
    for line in f:
      latency_90th= float(line.split(',')[9])
      if latency_90th>1000:
        missed_count+=1
  return missed_count

base_dir='/home/kharesp/static_placement/runtime/skewed/varying_k'
results_dir='/home/kharesp/static_placement/runtime/skewed/varying_k/topics_summary'

for curr_k in k:
  with open('%s/k_%d'%(results_dir,curr_k),'w') as f:
    f.write(','.join(algorithms)+'\n')
    for iteration in range(iterations):
      res=''
      for algo in algorithms:
        latency_file='%s/k_%d/n_50/%d/summary/summary_topic.csv'%\
          (base_dir,curr_k,iteration+1)
        missed_count= number_of_topics_with_missed_deadline(latency_file)
        res=res+'%d,'%(missed_count)
      f.write(res.rstrip(',')+'\n')
