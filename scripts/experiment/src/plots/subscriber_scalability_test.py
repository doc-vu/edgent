import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import argparse 

#function for extracting edge broker utilization metrics. 
#returns {'eb':{'cpu':%,'mem':%,'nw':kB/sec}} mapping
#for all broker machines
def util(log_dir,min_sub,step_size,max_sub,eb):
  broker_util_map={}
  for i in range(min_sub,max_sub+step_size,step_size):
    util_file='%s/%d/summary/summary_util.csv'%(log_dir,i)
    with open(util_file) as f:
      #skip header
      next(f)
      for line in f:
        #if line.startswith(eb):
        hostname,cpu,mem,nw= line.split(',')
        if hostname not in broker_util_map:
          broker_util_map[hostname]={'cpu':[float(cpu)],'mem':[float(mem)],'nw':[float(nw)]}
        else:
          broker_util_map[hostname]['cpu'].append(float(cpu))
          broker_util_map[hostname]['mem'].append(float(mem))
          broker_util_map[hostname]['nw'].append(float(nw))
  
  return broker_util_map

#function for extracting topic average,90th and 99th percentile latency 
#returns {'topic': {'avg':l,'90th':l,'99th':l}} mapping
def latency(log_dir,min_sub,step_size,max_sub):
  topic_latency_map={}
  for i in range(min_sub,max_sub+step_size,step_size):
    latency_file='%s/%d/summary/summary_topic.csv'%(log_dir,i)
    with open(latency_file) as f:
      #skip header
      next(f)
      for line in f:
        topic,num_sub,\
        avg_latency,min_latency,max_latency,\
        latency_90_percentile,latency_99_percentile,\
        latency_99_99_percentile,latency_99_9999_percentile= line.split(',')
        if topic not in topic_latency_map:
          topic_latency_map[topic]={'avg':[float(avg_latency)],\
            '90th':[float(latency_90_percentile)],\
            '99th':[float(latency_99_percentile)]}
        else:
          topic_latency_map[topic]['avg'].append(float(avg_latency))
          topic_latency_map[topic]['90th'].append(float(latency_90_percentile))
          topic_latency_map[topic]['99th'].append(float(latency_99_percentile))
  
  return topic_latency_map

def plot(log_dir,min_sub,step_size,max_sub,eb):
  #get broker to utilization map
  broker_util_map= util(log_dir,min_sub,step_size,max_sub,eb)
  #get topic to latency map
  topic_latency_map= latency(log_dir,min_sub,step_size,max_sub)
  subscribers=range(min_sub,max_sub+step_size,step_size)
  
  #plots the resource utilization for a broker
  #util_broker_cpu.png, util_broker_mem.png and util_broker_nw.png
  for broker,utilization in broker_util_map.items():
    for resource in utilization:
      yvals=utilization[resource]
      plt.plot(subscribers,yvals,marker='*')
      plt.xlabel('#subscribers')
      plt.ylabel('%s %s'%(broker,resource))
      for i, val in enumerate(yvals):
        plt.annotate('%.1f'%(val),(subscribers[i],yvals[i]),\
          verticalalignment='bottom',horizontalalignment='top',size=13)
      plt.savefig('%s/util_%s_%s.png'%(log_dir,broker,resource))
      plt.close()


  #Plots the average, 90th and 99th percentile latencies for each topic
  #latency_topic.png
  markers={'avg':'*','90th':'^','99th':'s'}
  colors={'avg':'b','90th':'g','99th':'r'}
  for topic,latencies in topic_latency_map.items():
    for measurement in latencies:
      yvals=latencies[measurement]
      plt.plot(subscribers,yvals,\
        marker=markers[measurement],\
        color=colors[measurement],
        label='%s %s'%(topic,measurement))
      
    plt.xlabel('#subscribers')
    plt.ylabel('latency(ms)')
    plt.legend()
    plt.savefig('%s/latency_%s.png'%(log_dir,topic))
    plt.close()

  #Plots average latencies for each topic in latency_average.png
  for topic,latencies in topic_latency_map.items():
      yvals=latencies['avg']
      plt.plot(subscribers,yvals,\
        marker='*',label='%s'%(topic))
      for i, val in enumerate(yvals):
        plt.annotate('%.1f'%(val),(subscribers[i],yvals[i]),\
          verticalalignment='bottom',horizontalalignment='top',size=11)
      
  plt.xlabel('#subscribers')
  plt.ylabel('average latency(ms)')
  plt.legend()
  plt.savefig('%s/latency_average.png'%(log_dir))
  plt.close()

if __name__=="__main__":
  #parse cmd line args
  parser=argparse.ArgumentParser(description='script for plotting results for subscriber scalability test')
  parser.add_argument('log_dir',help='log directory in which subscriber scalability test data resides')
  parser.add_argument('min_sub',type=int,help='min subscriber count with which the scalability test begins')
  parser.add_argument('step_size',type=int,help='step size used for increasing the #subscribers count')
  parser.add_argument('max_sub',type=int,help='max subscriber count at which the scalability test ends')
  parser.add_argument('eb',help='name of hostmachine hosting the EB')
  args=parser.parse_args()

  #collate results and plot
  plot(args.log_dir,args.min_sub,args.step_size,args.max_sub,args.eb)
