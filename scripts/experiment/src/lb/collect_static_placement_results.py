import numpy as np
algorithms=['ffd','fs','hyb']
n=[10,20,30,40,50]
iterations=5

results_dir='/home/kharesp/static_placement/results/skewed/varying_n'
algo_fd={}
for topic_count in n:
  with open('%s/summary/n_%d'%(results_dir,topic_count),'w') as f:
    header=''
    for algo in algorithms:
      algo_fd[algo]= open('%s/%s/n_%d'%(results_dir,algo,topic_count),'r')
      header= header + '%s_b,%s_t,'%(algo,algo)
    f.write(header.rstrip(',')+'\n')
    for i in range(iterations):
      output=''
      for algo in algorithms:
        output=output+next(algo_fd[algo]).rstrip()+','
      f.write(output.rstrip(',')+'\n')
    for algo in algorithms:
      algo_fd[algo].close()
