import numpy as np
import os
algorithms=['hyb']
k=[1,2,3,4,5,6]
iterations=5
deadline=1000

def percentage_of_missed_deadline(dir_path):
  total_samples=0
  missed_deadline=0
  files=[os.path.join(dir_path,f) for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path,f))]
  for latency_file in files:
    with open(latency_file,'r') as f:
      next(f) #skip header
      for line in f:
        parts=line.split(',')
        total_samples+=1
        if (int(parts[4])>deadline):
          missed_deadline+=1
  return  (missed_deadline*100)/total_samples

base_dir='/home/kharesp/static_placement/runtime/skewed/varying_k'
results_dir='/home/kharesp/static_placement/runtime/skewed/varying_k/deadline_summary'

for curr_k in k:
  with open('%s/k_%d'%(results_dir,curr_k),'w') as f:
    f.write(','.join(algorithms)+'\n')
    for iteration in range(iterations):
      res=''
      for algo in algorithms:
        dir_path='%s/k_%d/n_50/%d/filtered'%(base_dir,curr_k,iteration+1)
        percent= percentage_of_missed_deadline(dir_path)
        res=res+'%f,'%(percent)
      f.write(res.rstrip(',')+'\n')
