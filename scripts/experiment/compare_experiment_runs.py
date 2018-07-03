log_dir='/home/kharesp/workspace/java/edgent/model_learning/training/data/5_colocation'
k=5
runs=3
n=200

with open('%s/comparision_of_runs.csv'%(log_dir),'w') as f:
  f.write(','.join(['l%d'%(i+1) for i in range(runs)])+'\n')
  for idx in range(1,n+1):
    files=[open('%s/run%d/%d/summary/summary_topic.csv'%(log_dir,i+1,idx),'r')\
      for i in range(1,runs)]

    topic_latency={}

    for fd in files:
      next(fd)
      for line in fd:
        parts=line.split(',')
        topic= parts[0]
        latency= parts[9]
        if topic in topic_latency:
          topic_latency[topic].append(latency)
        else:
          topic_latency[topic]=[latency]

    for topic in ['t%d'%(i+1) for i in range(k)]:
      f.write(','.join(topic_latency[topic])+'\n')

    for fd in files:
      fd.close()
