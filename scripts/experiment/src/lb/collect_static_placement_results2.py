import numpy as np
algorithms=['hyb']
k=[1,2,3,4,5,6]
iterations=5

def avg_90th_percentile_latency_system(path):
  values=[]
  with open(path,'r') as f:
    next(f) 
    for line in f:
      values.append(float(line.split(',')[9]))
  return np.mean(values)

base_dir='/home/kharesp/static_placement/runtime/skewed/varying_k'
results_dir='/home/kharesp/static_placement/runtime/skewed/varying_k/summary'

for  curr_k in k:
  with open('%s/k_%d'%(results_dir,curr_k),'w') as f:
    f.write(','.join(algorithms)+'\n')
    for iteration in range(iterations):
      res=''
      for algo in algorithms:
        latency_file='%s/k_%d/n_50/%d/summary/summary_topic.csv'%\
          (base_dir,curr_k,iteration+1)
        avg_latency= avg_90th_percentile_latency_system(latency_file)
        res=res+'%f,'%(avg_latency)
      f.write(res.rstrip(',')+'\n')
