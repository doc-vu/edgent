import matplotlib.pyplot as plt
import numpy as np

log_dir='/home/kharesp/log/final_results/payload_rates_test'
payload_sizes=[125,250,500,1000,2000,4000,8000,16000,32000,64000]
data_rates=[10,50,100,200]

#####Process latency
latency={}
latency_to_eb={}
latency_from_eb={}
for rate in data_rates:
  for payload in payload_sizes:
    with open('%s/rate_%d/%d/summary/summary_topic.csv'%\
      (log_dir,rate,payload),'r') as f:
      #skip header
      next(f)
      for line in f:
        topic,num_sub,\
        avg_latency,min_latency,max_latency,\
        latency_90_percentile,latency_99_percentile,\
        latency_99_99_percentile,latency_99_9999_percentile,\
        avg_latency_to_eb,avg_latency_from_eb= line.split(',')
        if(topic not in latency):
          latency[topic]={r:[] for r in data_rates}
          latency_to_eb[topic]={r:[] for r in data_rates}
          latency_from_eb[topic]={r:[] for r in data_rates}
          latency[topic][rate].append(float(avg_latency))
          latency_to_eb[topic][rate].append(float(avg_latency_to_eb))
          latency_from_eb[topic][rate].append(float(avg_latency_from_eb))
        else:
          latency[topic][rate].append(float(avg_latency))
          latency_to_eb[topic][rate].append(float(avg_latency_to_eb))
          latency_from_eb[topic][rate].append(float(avg_latency_from_eb))

with open('%s/summary_topic.csv'%(log_dir), 'w') as f:
  for topic,rate_values_map in latency.iteritems():
    for rate,values in rate_values_map.iteritems():
      f.write('%s,%d,latency(ms),%s\n'%(topic,rate,\
        ','.join([str(val) for val in values])))
    f.write('\n')

  for topic,rate_values_map in latency_to_eb.iteritems():
    for rate,values in rate_values_map.iteritems():
      f.write('%s,%d,latency_to_eb(ms),%s\n'%(topic,rate,\
        ','.join([str(val) for val in values])))
    f.write('\n')

  for topic,rate_values_map in latency_from_eb.iteritems():
    for rate,values in rate_values_map.iteritems():
      f.write('%s,%d,latency_from_eb(ms),%s\n'%(topic,rate,\
        ','.join([str(val) for val in values])))
    f.write('\n')

def bar_plot_latency(latency_map,ylabel):
  n_groups=len(payload_sizes)
  index=np.arange(n_groups)
  bar_width=.2
  opacity=.4
  colors=['y','g','b','r']
  def autolabel(rects):
    for rect in rects:
      height = rect.get_height()
      ax.text(rect.get_x() + rect.get_width()/2., 1.05*height,
        '%.1f'%height,ha='center', va='bottom',size=5)

  for topic in latency_map:
    fig,ax= plt.subplots()
    for idx,rate in enumerate(data_rates):
      res=plt.bar(index+idx*bar_width,
        latency_map[topic][rate],bar_width,
        alpha=opacity,color=colors[idx],label='rate:%d'%(rate))
      autolabel(res)
   
    plt.rc('xtick',labelsize=10)
    plt.rc('ytick',labelsize=10)
    plt.xticks(index+bar_width/2,[str(size) for size in payload_sizes])
    plt.xlabel('payload sizes',size=7)
    plt.ylabel(ylabel,size=7)
    plt.legend(loc='upper center',bbox_to_anchor=(0.5,-0.08),shadow=True,
      fancybox=True,ncol=4,prop={'size':7})
    plt.tight_layout()
    plt.savefig('%s/%s_%s.pdf'%(log_dir,topic,ylabel))
    plt.close()

bar_plot_latency(latency,'latency')
bar_plot_latency(latency_to_eb,'latency_to_eb')
bar_plot_latency(latency_from_eb,'latency_from_eb')

#####Process utilization
rates_util={}
for rate in data_rates:
  rates_util[rate]={'sub':{'cpu':[],'iowait':[],'mem':[],'nw':[]},
    'eb':{'cpu':[],'iowait':[],'mem':[],'nw':[]},
    'pub':{'cpu':[],'iowait':[],'mem':[],'nw':[]}}

  for payload in payload_sizes:
    with open('%s/rate_%d/%d/summary/summary_util_by_role.csv'%\
      (log_dir,rate,payload),'r') as f:
      #skip header
      next(f)
      for line in f:
        role,cpu,iowait,mem,nw=line.split(',')
        rates_util[rate][role]['cpu'].append(float(cpu))
        rates_util[rate][role]['iowait'].append(float(iowait))
        rates_util[rate][role]['mem'].append(float(mem))
        rates_util[rate][role]['nw'].append(float(nw))

with open('%s/summary_util.csv'%(log_dir), 'w') as f:
  for rate in data_rates:
    for role in ['pub','eb','sub']:
      for resource in ['cpu','iowait','mem','nw']:
        f.write('%d,%s,%s,%s\n'%(rate,role,resource,\
          ','.join(str(item) for item in rates_util[rate][role][resource])))
      f.write('\n')
    f.write('\n')

#plot utilization bar plots
for role in ['sub','eb','pub']:
  for resource in ['cpu','iowait','mem','nw']:
    n_groups=len(payload_sizes)
    fig,ax= plt.subplots()
    index=np.arange(n_groups)
    bar_width=.2
    opacity=.4
    colors=['y','g','b','r']
    def autolabel(rects):
      for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width()/2., 1.05*height,
          '%.1f'%height,ha='center', va='bottom',size=5)
    
    for idx,rate in enumerate(data_rates):
      res=plt.bar(index+idx*bar_width,
        rates_util[rate][role][resource],bar_width,
        alpha=opacity,color=colors[idx],label='rate:%d'%(rate))
      if(not resource=='nw'):
        autolabel(res)

    plt.rc('xtick',labelsize=10)
    plt.rc('ytick',labelsize=10)
    plt.xlabel('payload sizes',size=7)
    plt.ylabel('%s_%s'%(role,resource),size=7)
    plt.xticks(index+bar_width/2,[str(size) for size in payload_sizes])
    plt.legend(loc='upper center',bbox_to_anchor=(0.5,-0.08),shadow=True,
      fancybox=True,ncol=4,prop={'size':7})
    plt.tight_layout()
    plt.savefig('%s/%s_%s.pdf'%(log_dir,role,resource))
    plt.close()
