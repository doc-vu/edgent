import matplotlib.pyplot as plt
import numpy as np

latency_avg_r1=[]
latency_90th_r1=[52.0, 49.0, 53.0, 47.0, 54.0, 51.0, 48.0, 55.0, 51.0, 47.0, 52.0, 52.0, 47.0, 46.0, 46.0, 46.0, 45.0, 52.0, 52.0, 54.0, 53.0, 52.0, 44.0, 46.0, 53.0, 56.0, 55.0, 58.0, 52.0, 51.0, 52.0, 51.0, 51.0, 57.0, 54.0, 52.0, 52.0, 56.0, 54.0, 54.0, 49.0, 51.0, 56.0, 51.0, 50.0, 54.0, 51.0, 50.0, 59.0, 51.0, 49.0, 54.0, 49.0, 55.0, 52.0, 54.0, 51.0, 56.0, 53.0, 55.0, 45.0, 54.0, 55.0, 47.0, 53.0, 57.0, 54.0, 56.0, 55.0, 51.0, 52.0, 54.0, 53.0, 53.0, 48.0, 57.0, 44.0, 51.0, 54.0, 46.0, 50.0, 56.0, 54.0, 50.0, 51.0, 53.0, 54.0, 51.0, 55.0, 59.0, 50.0, 48.0, 48.0, 53.0, 45.0, 50.0, 45.0, 61.0, 52.0, 55.0]

plt.scatter(np.arange(len(latency_90th_r1)),latency_90th_r1)
plt.savefig('scatter.pdf')
plt.close()

latency_avg_r2=[]
latency_90th_r2=[]

latency_avg_r3=[]
latency_90th_r3=[]

all_latency_avg=[latency_avg_r1]
all_latency_90th=[latency_90th_r1]


latency_avg=np.mean(all_latency_avg,axis=0)
latency_90th=np.mean(all_latency_90th,axis=0)

xvals=[0,20,40,60,80,100,120,140,160,180,200]
xlabel="loading topic's subscription size"
legend="t1 latency(ms)"

#plot average latency
yvals=latency_avg
plt.plot(xvals,yvals,\
  marker='*',color='b',label='%s'%(legend))
for i, val in enumerate(yvals):
  if (i%2==0):
    plt.annotate('%.1f'%(val),(xvals[i],yvals[i]),\
      verticalalignment='top',horizontalalignment='top',size=9)

plt.xlabel(xlabel)
plt.ylabel('average latency(ms)')
plt.legend()
plt.savefig('latency_average.pdf')
plt.close()

#plot 90th percentile latency
yvals=latency_90th
plt.plot(xvals,yvals,\
  marker='*',color='r',label='%s'%(legend))
for i, val in enumerate(yvals):
  if (i%2==0):
    plt.annotate('%.1f'%(val),(xvals[i],yvals[i]),\
      verticalalignment='top',horizontalalignment='top',size=9)

plt.xlabel(xlabel)
plt.ylabel('latency 90th percentile(ms)')
plt.legend()
plt.savefig('latency_90th.pdf')
plt.close()
