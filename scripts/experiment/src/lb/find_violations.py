
def find_fraction_of_topics_with_missed_deadlines(log_dir,deadline):
  all_topics=[]
  topics_missed_deadlines=[]
  with open('%s/summary/summary_topic.csv'%(log_dir),'r') as f:
    next(f) #skip header
    for line in f:
      parts=line.split(',')
      topic_name=parts[0]
      latency_90th_percentile=parts[9]
      all_topics.append(topic_name)
      if (float(latency_90th_percentile)>deadline):
        topics_missed_deadlines.append(topic_name)
  #return len(topics_missed_deadlines)/len(all_topics) 
  return topics_missed_deadlines

deadline=1000
iterations=5
n=[10,20,30,40,50]
algorithms=['ffd','fs','hyb']

for algo in algorithms:
  print('\n\n\nFor Algorithm:%s'%(algo))
  log_dir='/home/kharesp/static_placement/runtime/skewed/varying_n/%s'%(algo)
  
  for topic in n:
    print('Current Topic count:%d'%(topic)) 
    for idx in range(iterations):
      curr_dir='%s/n_%d/%d'%(log_dir,topic,idx+1)
      #print(find_fraction_of_topics_with_missed_deadlines(curr_dir,1000)*100)
      print(find_fraction_of_topics_with_missed_deadlines(curr_dir,deadline))
