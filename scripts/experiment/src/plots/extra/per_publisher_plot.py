import numpy as np
import matplotlib.pyplot as plt
import os,argparse

def process(log_dir):
  for f in os.listdir(log_dir):
    if (os.path.isfile(os.path.join(log_dir,f)) and f.startswith('t')):
      process_topic_file(log_dir,f)

def process_topic_file(log_dir,topic_file):
  #create output directory
  topic=topic_file.split('_')[0]
  if not os.path.exists('%s/plots/%s'%(log_dir,topic)):
    os.makedirs('%s/plots/%s'%(log_dir,topic))
 
  os.makedirs('%s/plots/%s/values/latency'%(log_dir,topic))
  os.makedirs('%s/plots/%s/values/latency_to_eb'%(log_dir,topic))
  os.makedirs('%s/plots/%s/values/latency_from_eb'%(log_dir,topic))
  os.makedirs('%s/plots/%s/histogram/latency'%(log_dir,topic))
  os.makedirs('%s/plots/%s/histogram/latency_to_eb'%(log_dir,topic))
  os.makedirs('%s/plots/%s/histogram/latency_from_eb'%(log_dir,topic))

  publisher_latency={}
  publisher_latency_to_eb={}
  publisher_latency_from_eb={}
  
  with open('%s/%s'%(log_dir,topic_file),'r') as f:
    #skip header
    next(f)
    for line in f:
      reception_ts,container_id,sample_id,sender_ts,latency,\
      eb_receive_ts,latency_to_eb,latency_from_eb=line.split(',')
      if (container_id in publisher_latency):
        publisher_latency[container_id].append(float(latency))
        publisher_latency_to_eb[container_id].append(float(latency_to_eb))
        publisher_latency_from_eb[container_id].append(float(latency_from_eb))
      else:
        publisher_latency[container_id]=[float(latency)]
        publisher_latency_to_eb[container_id]=[float(latency_to_eb)]
        publisher_latency_from_eb[container_id]=[float(latency_from_eb)]
 
  hist_bins=[0,5,10,15,20,25,30] 
  for publisher in publisher_latency:
    #plot latency to sampleId plot 'publisher_latency.png'
    xvals=np.arange(len(publisher_latency[publisher]))  
    yvals=publisher_latency[publisher]
    plt.plot(xvals,yvals,color='b',alpha=.4)
    plt.title('%s'%(publisher))
    plt.xlabel('sample id')
    plt.ylabel('latency(ms)')
    plt.savefig('%s/plots/%s/values/latency/%s_latency.png'%(log_dir,topic,publisher))
    plt.close()

    max_val=np.max(yvals)
    if(max_val>np.max(hist_bins)):
      hist_bins.append(max_val)
    hist,bins=np.histogram(yvals,bins=hist_bins)
    plot_histogram(bins,hist,'%s/plots/%s/histogram/latency/%s_hist_latency.png'%(log_dir,topic,publisher),'b','latency frequency',publisher)

    #plot latency to eb plot 'publisher_latency_to_eb.png' 
    yvals=publisher_latency_to_eb[publisher]
    plt.plot(xvals,yvals,color='g',alpha=.4)
    plt.title('%s'%(publisher))
    plt.xlabel('sample id')
    plt.ylabel('latency to eb(ms)')
    plt.savefig('%s/plots/%s/values/latency_to_eb/%s_latency_to_eb.png'%(log_dir,topic,publisher))
    plt.close()

    del hist_bins[-1]
    max_val=np.max(yvals)
    if(max_val>np.max(hist_bins)):
      hist_bins.append(max_val)
    hist,bins=np.histogram(yvals,bins=hist_bins)
    plot_histogram(bins,hist,'%s/plots/%s/histogram/latency_to_eb/%s_hist_latency_to_eb.png'%(log_dir,topic,publisher),'g','latency_to_eb frequency',publisher)

    #plot latency from eb plot 'publisher_latency_from_eb.png' 
    yvals=publisher_latency_from_eb[publisher]
    plt.plot(xvals,yvals,color='r',alpha=.4)
    plt.title('%s'%(publisher))
    plt.xlabel('sample id')
    plt.ylabel('latency from eb(ms)')
    plt.savefig('%s/plots/%s/values/latency_from_eb/%s_latency_from_eb.png'%(log_dir,topic,publisher))
    plt.close()

    del hist_bins[-1]
    max_val=np.max(yvals)
    if(max_val>np.max(hist_bins)):
      hist_bins.append(max_val)
    hist,bins=np.histogram(yvals,bins=hist_bins)
    plot_histogram(bins,hist,'%s/plots/%s/histogram/latency_from_eb/%s_hist_latency_from_eb.png'%(log_dir,topic,publisher),'r','latency_from_eb frequency',publisher)

def plot_histogram(bins,hist,path,color_str,ylabel,title):
    width=.25
    fig,ax= plt.subplots()
    ind=np.arange(len(hist))
    rects=ax.bar(ind,hist,width,color=color_str,alpha=.4)
    ax.set_title(title)
    ax.set_xticks(ind)
    ax.set_xticklabels(['%d-%d'%(bins[i],bins[i+1]) for i in range(len(bins)-1)])
    ax.set_xlabel('latency value bins(ms)')
    ax.set_ylabel(ylabel)
    for rect in rects:
      height=rect.get_height()
      ax.text(rect.get_x()+rect.get_width()/2.,1.05*height,'%d'%int(height),ha='center',va='bottom')
    plt.savefig(path)
    plt.close()

if __name__== "__main__":
  #parse cmd line args
  parser=argparse.ArgumentParser(description='script for plotting latency values per publisher ')
  parser.add_argument('log_dir',help='path to log directory')
  parser.add_argument('min_count',type=int,help='starting count')
  parser.add_argument('step_size',type=int,help='step size with which to iterate')
  parser.add_argument('max_count',type=int,help='ending count')
  args=parser.parse_args()

  for i in range(args.min_count,args.max_count+args.step_size,args.step_size):
    #ensure log_dir/topic/plots and log_dir/topic/summary directories exist
    if not os.path.exists('%s/%d/plots'%(args.log_dir,i)):
      os.makedirs('%s/%d/plots'%(args.log_dir,i))
    #process latency files in log_dir/i
    process('%s/%d'%(args.log_dir,i))
