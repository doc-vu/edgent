import argparse,os,sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import metadata
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def process(log_dir):
  topic_files_map={}
  for f in os.listdir(log_dir):
    if(os.path.isfile(os.path.join(log_dir,f)) and f.startswith('t')):
      topic=f.partition('_')[0]
      if topic in topic_files_map:
        topic_files_map[topic].append(log_dir+'/'+f)
      else:
        topic_files_map[topic]=[log_dir+'/'+f]
  
  with open('%s/summary/summary_topic.csv'%(log_dir),'w') as f:
    header="""topic,#subscribers,\
avg_latency(ms),\
min_latency(ms),\
max_latency(ms),\
90th_percentile_latency(ms),\
99th_percentile_latency(ms),\
99.99th_percentile_latency(ms),\
99.9999th_percentile_latency(ms),\
avg_latency_to_eb(ms),\
avg_latency_from_eb(ms)\n"""
    f.write(header)

    for topic,files in topic_files_map.items():
      stats=process_topic(log_dir,topic,files)
      f.write('%s,%d,%f,%f,%f,%f,%f,%f,%f,%f,%f\n'%(topic,
        len(files),
        stats['latency_avg'],
        stats['latency_min'],
        stats['latency_max'],
        stats['latency_90th'],
        stats['latency_99th'],
        stats['latency_99_99th'],
        stats['latency_99_9999th'],
        stats['avg_latency_to_eb'],
        stats['avg_latency_from_eb']))

  with open('%s/summary/overall_performance.csv'%(log_dir),'w') as f:
    data=np.genfromtxt('%s/summary/summary_topic.csv'%(log_dir),
      dtype='float,float,float,float,float,float,float,float,float',
      delimiter=',',skip_header=1,usecols=[2,3,4,5,6,7,8,9,10])
    latency_avg=np.mean(data['f0'])
    latency_min=np.mean(data['f1'])
    latency_max=np.mean(data['f2'])
    latency_90th=np.mean(data['f3'])
    latency_99th=np.mean(data['f4'])
    latency_99_99th=np.mean(data['f5'])
    latency_99_9999th=np.mean(data['f6'])
    latency_to_eb=np.mean(data['f7'])
    latency_from_eb=np.mean(data['f8'])
    header="""avg_latency(ms),\
min_latency(ms),\
max_latency(ms),\
90th_percentile_latency(ms),\
99th_percentile_latency(ms),\
99.99th_percentile_latency(ms),\
99.9999th_percentile_latency(ms),\
avg_latency_to_eb(ms),\
avg_latency_from_eb(ms)\n"""
    f.write(header)
    f.write('%f,%f,%f,%f,%f,%f,%f,%f,%f\n'%(latency_avg,latency_min,latency_max,
      latency_90th,latency_99th,latency_99_99th,
      latency_99_9999th,latency_to_eb,latency_from_eb))

def process_topic(log_dir,topic,topic_files):
  #extract latency values 
  data=[np.genfromtxt(f,dtype='int,int,int',delimiter=',',\
    usecols=[4,6,7],skip_header=1)[metadata.initial_samples:] for f in topic_files]
  
  latency=reduce(lambda x,y:x+y, [data[i]['f0'] for i in range(len(data))])/(len(data)*1.0)
  latency_to_eb=reduce(lambda x,y:x+y, [data[i]['f1'] for i in range(len(data))])/(len(data)*1.0)
  latency_from_eb=reduce(lambda x,y:x+y, [data[i]['f2'] for i in range(len(data))])/(len(data)*1.0)
 
  with open('%s/summary/%s.csv'%(log_dir,topic),'w') as f:
    f.write('avg latency(ms),avg latency_to_eb(ms),avg latency_from_eb(ms)\n')
    for i in range(len(latency)):
      f.write('%f,%f,%f\n'%(latency[i],latency_to_eb[i],latency_from_eb[i]))

  #plot latency values
  xvals=np.arange(len(latency))
  plt.plot(xvals,latency)
  plt.xticks(rotation=30)
  plt.xlabel('sample id')
  plt.ylabel('latency (ms)')
  plt.savefig('%s/plots/latency_%s.png'%(log_dir,topic))
  plt.close()

  #plot latency_to_eb values
  plt.plot(xvals,latency_to_eb)
  plt.xticks(rotation=30)
  plt.xlabel('sample id')
  plt.ylabel('latency to EB (ms)')
  plt.savefig('%s/plots/latency_to_eb_%s.png'%(log_dir,topic))
  plt.close()

  #plot latency_from_eb values
  plt.plot(xvals,latency_from_eb)
  plt.xticks(rotation=30)
  plt.xlabel('sample id')
  plt.ylabel('latency from EB (ms)')
  plt.savefig('%s/plots/latency_from_eb_%s.png'%(log_dir,topic))
  plt.close()

  sorted_latency_vals=np.sort(latency)

  #plot cdf
  yvals=np.arange(1,len(sorted_latency_vals)+1)/float(len(sorted_latency_vals))
  plt.plot(sorted_latency_vals,yvals)
  plt.xlabel('latency(ms)')
  plt.ylabel('cdf')
  plt.savefig('%s/plots/cdf_%s.png'%(log_dir,topic))
  plt.close()

  #plot histogram of latency values
  max_latency=np.max(sorted_latency_vals)
  hist_bins=[0,5,10,15,20,25,30]
  if(max_latency>30):
    hist_bins.append(max_latency)
  hist,bins=np.histogram(sorted_latency_vals,bins=hist_bins) 
  plot_histogram(bins,hist,'%s/plots/histogram_%s.png'%(log_dir,topic),'b','latency frequency')
  

  #mean,min,max,90th and 99th percentile latency values
  stats={}
  stats['latency_avg']= np.mean(sorted_latency_vals)
  stats['latency_min']= sorted_latency_vals[0] 
  stats['latency_max']= sorted_latency_vals[-1] 
  stats['latency_90th']= np.percentile(sorted_latency_vals,90)
  stats['latency_99th']= np.percentile(sorted_latency_vals,99)
  stats['latency_99_99th']= np.percentile(sorted_latency_vals,99.99)
  stats['latency_99_9999th']= np.percentile(sorted_latency_vals,99.9999)
  stats['avg_latency_to_eb']= np.mean(latency_to_eb)
  stats['avg_latency_from_eb']= np.mean(latency_from_eb)

  return stats


def plot_histogram(bins,hist,path,color_str,ylabel):
    width=.25
    fig,ax= plt.subplots()
    ind=np.arange(len(hist))
    rects=ax.bar(ind,hist,width,color=color_str,alpha=.4)
    ax.set_xticks(ind)
    ax.set_xticklabels(['%d-%d'%(bins[i],bins[i+1]) for i in range(len(bins)-1)],rotation=30)
    ax.set_xlabel('latency value bins(ms)')
    ax.set_ylabel(ylabel)
    for rect in rects:
      height=rect.get_height()
      ax.text(rect.get_x()+rect.get_width()/2.,1.05*height,'%d'%height,ha='center',va='bottom')
    plt.savefig(path)
    plt.close()

if __name__== "__main__":
  #parse cmd line args
  parser=argparse.ArgumentParser(description='script for processing topic latency files')
  parser.add_argument('-log_dir',help='path to log directory',required=True)
  parser.add_argument('-sub_dirs',nargs='*',required=True)
  args=parser.parse_args()

  for sub_dir in args.sub_dirs:
    #ensure log_dir/sub_dir/plots and log_dir/sub_dir/summary directories exist
    if not os.path.exists('%s/%s/plots'%(args.log_dir,sub_dir)):
      os.makedirs('%s/%s/plots'%(args.log_dir,sub_dir))
    if not os.path.exists('%s/%s/summary'%(args.log_dir,sub_dir)):
      os.makedirs('%s/%s/summary'%(args.log_dir,sub_dir))
    #process latency files in log_dir/i
    process('%s/%s'%(args.log_dir,sub_dir))
