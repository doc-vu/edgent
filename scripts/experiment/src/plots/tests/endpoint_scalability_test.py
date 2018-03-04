import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import argparse 

#function for extracting edge broker utilization metrics. 
#returns {'eb':{'cpu':%,'mem':%,'nw':kB/sec}} mapping
#for all broker machines
def util(log_dir,start_range,step_size,end_range,eb):
  broker_util_map={}
  for i in range(start_range,end_range+step_size,step_size):
    util_file='%s/%d/summary/summary_util.csv'%(log_dir,i)
    with open(util_file) as f:
      #skip header
      next(f)
      for line in f:
        if line.startswith(eb):
          hostname,cpu,iowait,mem,nw= line.split(',')
          if hostname not in broker_util_map:
            broker_util_map[hostname]={'cpu':[float(cpu)],\
              'iowait':[float(iowait)],'mem':[float(mem)],'nw':[float(nw)]}
          else:
            broker_util_map[hostname]['cpu'].append(float(cpu))
            broker_util_map[hostname]['iowait'].append(float(iowait))
            broker_util_map[hostname]['mem'].append(float(mem))
            broker_util_map[hostname]['nw'].append(float(nw))
  
  return broker_util_map

#function for extracting topic average,90th and 99th percentile latency 
#returns {'topic': {'avg':l,'90th':l,'99th':l}} mapping
def latency(log_dir,start_range,step_size,end_range):
  topic_latency_map={}
  for i in range(start_range,end_range+step_size,step_size):
    latency_file='%s/%d/summary/summary_topic.csv'%(log_dir,i)
    with open(latency_file) as f:
      #skip header
      next(f)
      for line in f:
        topic,num_sub,\
        avg_latency,min_latency,max_latency,\
        latency_90_percentile,latency_99_percentile,\
        latency_99_99_percentile,latency_99_9999_percentile,avg_latency_to_eb,avg_latency_from_eb= line.split(',')
        if topic not in topic_latency_map:
          topic_latency_map[topic]={'avg':[float(avg_latency)],\
            '90th':[float(latency_90_percentile)],\
            '99th':[float(latency_99_percentile)],\
            'avg_latency_to_eb':[float(avg_latency_to_eb)],\
            'avg_latency_from_eb':[float(avg_latency_from_eb)]}
        else:
          topic_latency_map[topic]['avg'].append(float(avg_latency))
          topic_latency_map[topic]['90th'].append(float(latency_90_percentile))
          topic_latency_map[topic]['99th'].append(float(latency_99_percentile))
          topic_latency_map[topic]['avg_latency_to_eb'].append(float(avg_latency_to_eb))
          topic_latency_map[topic]['avg_latency_from_eb'].append(float(avg_latency_from_eb))
  
  return topic_latency_map

def plot(log_dir,start_range,step_size,end_range,xlabel,eb):
  xvals=range(start_range,end_range+step_size,step_size)

  #get broker to utilization map
  broker_util_map= util(log_dir,start_range,step_size,end_range,eb)
  
  #plots the resource utilization for a broker
  #util_broker_cpu.png, util_broker_mem.png and util_broker_nw.png
  for broker,utilization in broker_util_map.items():
    for resource in utilization:
      yvals=utilization[resource]
      plt.plot(xvals,yvals,marker='*')
      plt.xlabel(xlabel)
      plt.ylabel('%s %s'%(broker,resource))
      for i, val in enumerate(yvals):
        plt.annotate('%.1f'%(val),(xvals[i],yvals[i]),\
          verticalalignment='bottom',horizontalalignment='top',size=13)
      plt.savefig('%s/util_%s_%s.png'%(log_dir,broker,resource))
      plt.close()

  #log observed utilization values on brokers for scalability test 
  with open('%s/summary_utilization_%s.csv'%(log_dir,eb),'w') as f:
    f.write('#%s:%s\n'%(xlabel,','.join(str(xval) for xval in xvals)))
    for broker,utilization in broker_util_map.items():
      for resource in utilization:
        yvals=utilization[resource]
        f.write('%s,%s,%s\n'%(broker,resource,','.join(str(val) for val in yvals)))
      f.write('\n')
      
  #get topic to latency map
  topic_latency_map= latency(log_dir,start_range,step_size,end_range)
  #log observed latency values for the scalability test 
  with open('%s/summary_latency.csv'%(log_dir),'w') as f:
    f.write('#%s:%s\n'%(xlabel,','.join(str(xval) for xval in xvals)))
    for topic,latencies in topic_latency_map.items():
      for measurement in latencies:
        yvals=latencies[measurement]
        f.write('%s,%s,%s\n'%(topic,measurement,','.join(str(val) for val in yvals)))

  #Plots the average, 90th and 99th percentile latencies for each topic
  #latency_topic.png
  markers={'avg':'*','90th':'^','99th':'s'}
  colors={'avg':'b','90th':'g','99th':'r'}
  measurements=['avg','90th','99th']
  for topic,latencies in topic_latency_map.items():
    for measurement in measurements:
      yvals=latencies[measurement]
      print(xvals)
      print(yvals)
      plt.plot(xvals,yvals,\
        marker=markers[measurement],\
        color=colors[measurement],
        label='%s %s'%(topic,measurement))
      for i, val in enumerate(yvals):
        plt.annotate('%.1f'%(val),(xvals[i],yvals[i]),\
          verticalalignment='bottom',horizontalalignment='top',size=11)
      
      plt.xlabel(xlabel)
      plt.ylabel('%s latency(ms)'%(measurement))
      plt.legend()
      plt.savefig('%s/latency_%s_%s.png'%(log_dir,measurement,topic))
      plt.close()

  #Plots average latencies for each topic in latency_average.png
  for topic,latencies in topic_latency_map.items():
      yvals=latencies['avg']
      plt.plot(xvals,yvals,\
        marker='*',label='%s'%(topic))
      for i, val in enumerate(yvals):
        plt.annotate('%.1f'%(val),(xvals[i],yvals[i]),\
          verticalalignment='bottom',horizontalalignment='top',size=11)
      
  plt.xlabel(xlabel)
  plt.ylabel('average latency(ms)')
  plt.legend()
  plt.savefig('%s/latency_average.png'%(log_dir))
  plt.close()

  #Plots average latency to eb for each topic in latency_to_eb_average.png
  for topic,latencies in topic_latency_map.items():
      yvals=latencies['avg_latency_to_eb']
      plt.plot(xvals,yvals,\
        marker='*',label='%s'%(topic))
      for i, val in enumerate(yvals):
        plt.annotate('%.1f'%(val),(xvals[i],yvals[i]),\
          verticalalignment='bottom',horizontalalignment='top',size=11)
      
  plt.xlabel(xlabel)
  plt.ylabel('average latency to EB(ms)')
  plt.legend()
  plt.savefig('%s/latency_to_eb_average.png'%(log_dir))
  plt.close()

  #Plots average latency from eb for each topic in latency_from_eb_average.png
  for topic,latencies in topic_latency_map.items():
      yvals=latencies['avg_latency_from_eb']
      plt.plot(xvals,yvals,\
        marker='*',label='%s'%(topic))
      for i, val in enumerate(yvals):
        plt.annotate('%.1f'%(val),(xvals[i],yvals[i]),\
          verticalalignment='bottom',horizontalalignment='top',size=11)
      
  plt.xlabel(xlabel)
  plt.ylabel('average latency from EB(ms)')
  plt.legend()
  plt.savefig('%s/latency_from_eb_average.png'%(log_dir))
  plt.close()

if __name__=="__main__":
  #parse cmd line args
  parser=argparse.ArgumentParser(description='script for plotting results for endpoint scalability test')
  parser.add_argument('log_dir',help='log directory in which scalability test data resides')
  parser.add_argument('start_range',type=int,help='starting range of scalability test')
  parser.add_argument('step_size',type=int,help='step size with which iteration happens')
  parser.add_argument('end_range',type=int,help='ending range of scalability test')
  parser.add_argument('xlabel',help='entity for which scalability test is being performed')
  parser.add_argument('eb',help='name of hostmachine hosting the EB')
  args=parser.parse_args()

  #collate results and plot
  plot(args.log_dir,args.start_range,args.step_size,args.end_range,args.xlabel,args.eb)
