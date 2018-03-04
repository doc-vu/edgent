import argparse,os,sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import conf
import numpy as np
import matplotlib.pyplot as plt

def process(log_dir,sub_dirs,xlabel):
  role_util_map= process_util(log_dir,sub_dirs)  
  log_util(log_dir,sub_dirs,role_util_map)
  plot_util(log_dir,sub_dirs,role_util_map,xlabel)

  topic_latency_map=process_latency(log_dir,sub_dirs)
  log_latency(log_dir,sub_dirs,topic_latency_map)
  plot_latency(log_dir,sub_dirs,topic_latency_map,xlabel)

  topic_rate_map=process_rates(log_dir,sub_dirs)
  log_rate(log_dir,sub_dirs,topic_rate_map)
  plot_rate(log_dir,sub_dirs,topic_rate_map,xlabel)
 
def process_util(log_dir,sub_dirs):
  role_util_map={'eb':{'cpu':[],'iowait':[],'mem':[],'nw':[]},
    'sub':{'cpu':[],'iowait':[],'mem':[],'nw':[]},
    'pub':{'cpu':[],'iowait':[],'mem':[],'nw':[]}}
  for sub_dir in sub_dirs:
    test_config=conf.Conf('%s/%s/conf'%(args.log_dir,sub_dir))
    ebs=test_config.ebs
    subscriber_client_machines=test_config.subscriber_client_machines
    publisher_client_machines=test_config.publisher_client_machines
    util_file='%s/%s/summary/summary_util.csv'%(log_dir,sub_dir)
    with open(util_file) as f:
      util={'eb':{'cpu':[],'iowait':[],'mem':[],'nw':[]},
        'sub':{'cpu':[],'iowait':[],'mem':[],'nw':[]},
        'pub':{'cpu':[],'iowait':[],'mem':[],'nw':[]}}
      #skip header  
      next(f)
      for line in f:
        hostname,cpu,iowait,mem,nw= line.split(',')
        if(hostname in ebs):
          util['eb']['cpu'].append(float(cpu))
          util['eb']['iowait'].append(float(iowait))
          util['eb']['mem'].append(float(mem))
          util['eb']['nw'].append(float(nw))
        if(hostname in subscriber_client_machines):
          util['sub']['cpu'].append(float(cpu))
          util['sub']['iowait'].append(float(iowait))
          util['sub']['mem'].append(float(mem))
          util['sub']['nw'].append(float(nw))
        if(hostname in publisher_client_machines):
          util['pub']['cpu'].append(float(cpu))
          util['pub']['iowait'].append(float(iowait))
          util['pub']['mem'].append(float(mem))
          util['pub']['nw'].append(float(nw))

      for role,utilization in util.items():
        for resource in utilization:
          role_util_map[role][resource].append(np.mean(utilization[resource]))

  return role_util_map

def process_latency(log_dir,sub_dirs):
  topic_latency_map={}
  for sub_dir in sub_dirs:
    latency_file='%s/%s/summary/summary_topic.csv'%(log_dir,sub_dir)
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

def process_rates(log_dir,sub_dirs):
  topic_rate_map={}
  for sub_dir in sub_dirs:
    rate_file='%s/%s/summary/summary_reception_rates.csv'%(log_dir,sub_dir)
    with open(rate_file) as f:
      #skip header
      next(f)
      for line in f:
        topic,sub_reception_rate,eb_reception_rate= line.split(',')
        if topic not in topic_rate_map:
          topic_rate_map[topic]={'sub_reception_rate':[float(sub_reception_rate)],\
            'eb_reception_rate':[float(eb_reception_rate)]}
        else:
          topic_rate_map[topic]['sub_reception_rate'].append(float(sub_reception_rate))
          topic_rate_map[topic]['eb_reception_rate'].append(float(eb_reception_rate))
  return topic_rate_map


def log_util(log_dir,sub_dirs,role_util_map):
 #log observed utilization values 
  with open('%s/summary_utilization.csv'%(log_dir),'w') as f:
    f.write('xticks:%s\n'%(','.join(sub_dir for sub_dir in sub_dirs)))
    for role,utilization in role_util_map.items():
      for resource in utilization:
        yvals=utilization[resource]
        f.write('%s,%s,%s\n'%(role,resource,','.join(str(val) for val in yvals)))
      f.write('\n')

def plot_util(log_dir,sub_dirs,role_util_map,xlabel):
  #plots the resource utilization bar plots roles: eb,pub,sub   
  for role,utilization in role_util_map.items():
    for resource in utilization:
      n_groups=len(sub_dirs)
      fig,ax = plt.subplots()
      index= np.arange(n_groups)
      bar_width= .2
      opacity= .4
      def autolabel(rects):
        for rect in rects:
          height = rect.get_height()
          ax.text(rect.get_x() + rect.get_width()/2., 1.05*height,
            '%.1f'%height,ha='center', va='bottom')

      pub= plt.bar(index,role_util_map['pub'][resource],bar_width,alpha=opacity,color='b',label='pub node')
      eb= plt.bar(index+ bar_width, role_util_map['eb'][resource],bar_width,alpha=opacity,color='g',label='eb node')
      sub= plt.bar(index+ 2*bar_width, role_util_map['sub'][resource],bar_width,alpha=opacity,color='r', label='sub node')
     
      if(not resource == 'nw'):
        autolabel(pub)
        autolabel(eb)
        autolabel(sub)
      
      plt.xlabel(xlabel)
      plt.ylabel('%s'%resource)
      plt.xticks(index+bar_width/2,sub_dirs)
      plt.legend(loc='best')
      plt.tight_layout()
      plt.savefig('%s/%s.pdf'%(log_dir,resource))
      plt.close()

def log_latency(log_dir,sub_dirs,topic_latency_map):
  #log observed latency values 
  with open('%s/summary_latency.csv'%(log_dir),'w') as f:
    f.write('xticks:%s\n'%(','.join(sub_dir for sub_dir in sub_dirs)))
    for topic,latencies in topic_latency_map.items():
      for measurement in latencies:
        yvals=latencies[measurement]
        f.write('%s,%s,%s\n'%(topic,measurement,','.join(str(val) for val in yvals)))

def plot_latency(log_dir,sub_dirs,topic_latency_map,xlabel):
  #Plots the average, 90th and 99th percentile latencies for each topic
  #in latency_topic.png
  markers={'avg':'*','90th':'^','99th':'s'}
  colors={'avg':'b','90th':'g','99th':'r'}
  measurements=['avg','90th','99th']
  xvals=range(len(sub_dirs))
  for topic,latencies in topic_latency_map.items():
    for measurement in measurements:
      yvals=latencies[measurement]
      plt.plot(xvals,yvals,\
        marker=markers[measurement],\
        color=colors[measurement],
        label='%s %s'%(topic,measurement))

      plt.xticks(xvals,sub_dirs,rotation=30)

      for i, val in enumerate(yvals):
        plt.annotate('%.1f'%(val),(xvals[i],yvals[i]),\
          verticalalignment='bottom',horizontalalignment='top',size=11)

    plt.xlabel(xlabel)
    plt.ylabel('latency(ms)')
    plt.legend()
    plt.savefig('%s/latency_%s.pdf'%(log_dir,topic))
    plt.close()

  #Plot average latencies for each topic in latency_average.png
  for topic,latencies in topic_latency_map.items():
    yvals=latencies['avg']
    plt.plot(xvals,yvals,\
      marker='*',label='%s'%(topic))
    for i, val in enumerate(yvals):
      plt.annotate('%.1f'%(val),(xvals[i],yvals[i]),\
        verticalalignment='bottom',horizontalalignment='top',size=11)

  plt.xticks(xvals,sub_dirs,rotation=30)
  plt.xlabel(xlabel)
  plt.ylabel('average latency(ms)')
  plt.legend()
  plt.savefig('%s/latency_average.pdf'%(log_dir))
  plt.close()

  #Plot average latency to EB for each topic in latency_to_EB.png
  for topic,latencies in topic_latency_map.items():
    yvals=latencies['avg_latency_to_eb']
    plt.plot(xvals,yvals,\
      marker='*',color='g',label='%s'%(topic))
    for i, val in enumerate(yvals):
      plt.annotate('%.1f'%(val),(xvals[i],yvals[i]),\
        verticalalignment='bottom',horizontalalignment='top',size=11)

  plt.xticks(xvals,sub_dirs,rotation=30)
  plt.xlabel(xlabel)
  plt.ylabel('average latency to EB(ms)')
  plt.legend()
  plt.savefig('%s/latency_to_Eb.pdf'%(log_dir))
  plt.close()

  #Plot average latency from EB for each topic in latency_from_EB.png
  for topic,latencies in topic_latency_map.items():
    yvals=latencies['avg_latency_from_eb']
    plt.plot(xvals,yvals,\
      marker='*',color='r',label='%s'%(topic))
    for i, val in enumerate(yvals):
      plt.annotate('%.1f'%(val),(xvals[i],yvals[i]),\
        verticalalignment='bottom',horizontalalignment='top',size=11)

  plt.xticks(xvals,sub_dirs,rotation=30)
  plt.xlabel(xlabel)
  plt.ylabel('average latency from EB(ms)')
  plt.legend()
  plt.savefig('%s/latency_from_Eb.pdf'%(log_dir))
  plt.close()
 
  #Bar plots per topic for average latency, latency to EB and latency from EB
  for topic,latencies in topic_latency_map.items():
    n_groups=len(sub_dirs)
    fig,ax = plt.subplots()
    index= np.arange(n_groups)
    bar_width= .2
    opacity= .4
    def autolabel(rects):
      for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width()/2., 1.05*height,
          '%.1f'%height,ha='center', va='bottom')
    
    l= plt.bar(index,latencies['avg'],bar_width,alpha=opacity,color='b',label='latency')
    autolabel(l)
    l_to_eb= plt.bar(index+ bar_width, latencies['avg_latency_to_eb'],
      bar_width,alpha=opacity,color='g',label='latency to eb')
    autolabel(l_to_eb)
    l_from_eb= plt.bar(index+ 2*bar_width,latencies['avg_latency_from_eb'],
      bar_width,alpha=opacity,color='r',label='latency from eb')
    autolabel(l_from_eb)
    
    plt.xlabel(xlabel)
    plt.ylabel('latency(ms)')
    plt.xticks(index+bar_width/2,sub_dirs)
    plt.legend(loc='best')
    plt.tight_layout()
    plt.savefig('%s/latency_%s_bar.pdf'%(log_dir,topic))
    plt.close()
    
def log_rate(log_dir,sub_dirs,topic_rate_map):
  #log observed latency values 
  with open('%s/summary_rates.csv'%(log_dir),'w') as f:
    f.write('xticks:%s\n'%(','.join(sub_dir for sub_dir in sub_dirs)))
    for topic,rates in topic_rate_map.items():
      for r in rates:
        yvals=rates[r]
        f.write('%s,%s,%s\n'%(topic,r,','.join(str(val) for val in yvals)))

def plot_rate(log_dir,sub_dirs,topic_rate_map,xlabel):
  #Bar plots per topic for EB and subscriber reception rates 
  for topic,rates in topic_rate_map.items():
    n_groups=len(sub_dirs)
    fig,ax = plt.subplots()
    index= np.arange(n_groups)
    bar_width= .2
    opacity= .4
    
    eb= plt.bar(index,rates['eb_reception_rate'],bar_width,alpha=opacity,color='g',label='eb reception rate')
    sub= plt.bar(index+ bar_width, rates['sub_reception_rate'],
      bar_width,alpha=opacity,color='r',label='sub reception rate')
    
    plt.xlabel(xlabel)
    plt.ylabel('rate (msg/sec)')
    plt.xticks(index+bar_width/2,sub_dirs)
    plt.legend(loc='best')
    plt.tight_layout()
    plt.savefig('%s/rates_%s.pdf'%(log_dir,topic))
    plt.close()

if __name__== "__main__":
  #parse cmd line args
  parser=argparse.ArgumentParser(description='script for collating results across test iterations')
  parser.add_argument('-log_dir',help='path to log directory',required=True)
  parser.add_argument('-sub_dirs',nargs='*',required=True)
  parser.add_argument('-xlabel',help='variation across iterations',required=True)
  args=parser.parse_args()
 
  process(args.log_dir,args.sub_dirs,args.xlabel)
