import matplotlib.pyplot as plt
import numpy as np

xvals=[10,20,30,40,50,60,70,80,90,100,110,120,130,140,150,160,170,180,190,200]

cpu_r1=[0.60498,0.500535,0.613077,0.858508,0.927085,1.060041,1.203388,1.358765,1.501066,1.524959,1.889393,1.86898,1.899639,2.019024,2.223871,2.280442,2.472881,2.461554,2.690407,1.692494]
mem_r1=[0.535862,0.568363,0.558306,0.564946,0.554916,0.540168,0.532602,0.529838,0.536016,0.540677,0.573076,0.582316,0.570185,0.562944,0.558414,0.566756,0.581703,0.592909,0.605091,0.644714]
nw_r1=[1911.740082,3679.359339,5367.194878,7040.989028,8790.026138,10598.546122,12344.914508,14169.412479,15876.035967,17597.018519,19122.35622,21040.699754,22388.682661,24420.358857,25886.98753,27562.199194,29959.340372,30682.3656,33061.637796,20108.208113]

cpu_r2=[]
mem_r2=[]
nw_r2=[]

cpu_r3=[]
mem_r3=[]
nw_r3=[]

all_cpu=[cpu_r1]
all_mem=[mem_r1]
all_nw=[nw_r1]


cpu=np.mean(all_cpu,axis=0)
mem=np.mean(all_mem,axis=0)
nw=np.mean(all_nw,axis=0)

xlabel="publishers"
legend="eb"

#plot cpu
yvals=cpu
plt.plot(xvals,yvals,\
  marker='*',color='r',alpha=.4,label='%s'%(legend))
for i, val in enumerate(yvals):
  if (i%3==0):
    plt.annotate('%.1f'%(val),(xvals[i],yvals[i]),\
      verticalalignment='bottom',horizontalalignment='top',size=11)

plt.xlabel(xlabel)
plt.ylabel('% cpu utilization')
plt.legend()
plt.savefig('cpu.pdf')
plt.close()

#plot memory
yvals=mem
plt.plot(xvals,yvals,\
  marker='*',color='g',alpha=.4,label='%s'%(legend))
for i, val in enumerate(yvals):
  if (i%3==0):
    plt.annotate('%.1f'%(val),(xvals[i],yvals[i]),\
      verticalalignment='bottom',horizontalalignment='top',size=11)

plt.xlabel(xlabel)
plt.ylabel('memory(Gb)')
plt.legend()
plt.savefig('mem.pdf')
plt.close()

#plot network
toGb=.000008
yvals=[yval*toGb for yval in nw]
plt.plot(xvals,yvals,\
  marker='*',color='b',alpha=.4,label='%s'%(legend))
for i, val in enumerate(yvals):
  if (i%3==0):
    plt.annotate('%.1f'%(val),(xvals[i],yvals[i]),\
      verticalalignment='bottom',horizontalalignment='top',size=11)

plt.xlabel(xlabel)
plt.ylabel('network(Gb/sec)')
plt.legend()
plt.savefig('nw.pdf')
plt.close()
