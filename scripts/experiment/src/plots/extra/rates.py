import matplotlib.pyplot as plt

log_dir='/home/kharesp/log/cluster/single_host_subscriber_scalability_iter2'
directory_prefix='rate_'
subscribers=[5,10,15,20]
rates=[50,100,200,400,800]

topics=['t1']
hosts=['node3','node4','node5']
resources=['cpu','mem','nw']

def plot_util_for_all_rates():
  for host in hosts:
    for resource in resources:
      colors=['b','g','c','k','m','r']
      fig,ax = plt.subplots()
      ax.set_xlabel('#host machines')
      ax.set_xticks(subscribers)
      ax.set_ylabel('%s_%s'%(host,resource))
      for rate in rates:
        with open('%s/%s%d/summary_utilization.csv'%(log_dir,directory_prefix,rate)) as f:
          for line in f:
            if line.startswith('%s,%s'%(host,resource)):
              ydata=[float(val) for val in line.split(',')[2:]]
              ax.plot(subscribers,ydata[:-2],\
                marker='*',color=colors.pop(0),label='rate:%d'%(rate))
      ax.yaxis.grid(True)
      ax.legend()
      plt.savefig('/home/kharesp/log/%s_%s.pdf'%(host,resource),\
        bbox_inches='tight')
      plt.close()
      

def plot_latency_for_all_rates():
  for topic in topics:
    colors=['b','g','c','k','m','r']
    fig,ax = plt.subplots()
    ax.set_xlabel('#host machines')
    ax.set_xticks(subscribers)
    ax.set_ylabel('latency (ms)')
    for rate in rates:
      with open('%s/%s%d/summary_latency.csv'%(log_dir,directory_prefix,rate)) as f:
        for line in f:
          if line.startswith('%s,avg'%(topic)):
            ydata=[float(val) for val in line.split(',')[2:]]
            ax.plot(subscribers,ydata[:-2],\
              marker='*',color=colors.pop(0),label='rate:%d'%(rate))
    ax.yaxis.grid(True)
    ax.legend()
    plt.savefig('/home/kharesp/log/%s.pdf'%(topic),\
      bbox_inches='tight')
    plt.close()

if __name__=="__main__":
  plot_latency_for_all_rates() 
  plot_util_for_all_rates()
