topic_count=[10,20,30,40,50]
algorithms=['ffd','fs','hyb']
iterations=5

for algo in algorithms:
  for n in topic_count:
    log_dir='/home/kharesp/static_placement/runtime/skewed/varying_n/%s/n_%d'%(algo,n)
    
    with open('%s/observed_%d'%(log_dir,n),'w') as f:
      for idx in range(iterations):
        observations={}
        with open('%s/%d/summary/summary_topic.csv'%(log_dir,idx+1),'r') as inp:
          next(inp) #skip header
          for line in inp:
            parts=line.split(',')
            observations[parts[0]]=float(parts[9])
    
        res_str=''
        for t in range(n):
          res_str=res_str+'t%d:%f,'%(t+1,observations['t%d'%(t+1)])
    
        f.write(res_str.rstrip(',')+'\n')
