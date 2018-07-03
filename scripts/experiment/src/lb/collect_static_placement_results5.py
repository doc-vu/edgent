import numpy as np
algorithms=['hyb']
k=[1,2,3,4,5,6]
iterations=5

placement_dir='/home/kharesp/static_placement/placement/skewed/varying_k'
base_dir='/home/kharesp/static_placement/runtime/skewed/varying_k'
results_dir='/home/kharesp/static_placement/runtime/skewed/varying_k/util_summary'

def get_placement_str(curr_k,iteration):
  with open('%s/k_%d/n_50'%(placement_dir,curr_k),'r') as f:
    for i in range(iteration): 
      continue
    return next(f)

def avg_broker_cpu_utilization(path,number_of_brokers):
  brokers=['node%d'%(i) for i in range(2,number_of_brokers+2)]
  values=[]
  with open(path,'r') as f:
    next(f) #skip header 
    for line in f:
      hostname,cpu,io,mem,nw= line.split(',')
      if hostname in brokers:
        values.append(float(cpu))
  return np.mean(values)


for curr_k in k:
  with open('%s/k_%d'%(results_dir,curr_k),'w') as f:
    f.write(','.join(algorithms)+'\n')
    for iteration in range(iterations):
      res=''
      for algo in algorithms:
        placement=get_placement_str(curr_k,iteration)
        util_file='%s/k_%d/n_50/%d/summary/summary_util.csv'%\
          (base_dir,curr_k,iteration+1)
        avg_cpu= avg_broker_cpu_utilization(util_file,len(placement.rstrip().split(';')))
        res=res+'%f,'%(avg_cpu)
      f.write(res.rstrip(',')+'\n')
