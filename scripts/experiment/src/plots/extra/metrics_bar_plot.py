import matplotlib.pyplot as plt
import numpy as np

publishers=[5,10,15,20,25,30,35,40,45,50,55,60]

#publishing_rate=[100*num_pub for num_pub in publishers]
publishing_rate=[3000 for num_pub in publishers]
eb_reception_rate=[]
sub_reception_rate=[]

latency=[25.214785,25.965896,25.616104,27.045355,25.838033,28.15767,25.583787,28.32894,29.107024,25.628412,27.659266,27.772969]
latency_to_eb=[9.783583,9.887263,10.2825,10.07303,10.469893,10.37947,10.677563,10.635223,10.84886,11.434507,11.492713,11.521517]
latency_from_eb=[15.431201,16.078633,15.333604,16.972325,15.36814,17.7782,14.906224,17.693716,18.258164,14.193905,16.166553,16.251452]

def autolabel(rects):
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width()/2., 1.05*height,
                '%d' % int(height),
                ha='center', va='bottom')

##plot rates
#n_groups=len(publishers)
#
#fig,ax = plt.subplots()
#index= np.arange(n_groups)
#bar_width= .2
#opacity= .4
#  
#pub_rate= plt.bar(index,publishing_rate,bar_width,alpha=opacity,color='b',label='publishing rate')
#eb_rate= plt.bar(index+ bar_width, eb_reception_rate,bar_width,alpha=opacity,color='g',label='eb reception rate')
#sub_rate= plt.bar(index+ 2*bar_width,sub_reception_rate, bar_width,alpha=opacity,color='r', label='sub reception rate')
#  
#plt.xlabel('subscribers')
#plt.ylabel('data rate (msgs/sec)')
#plt.xticks(index+bar_width/2,publishers)
#plt.legend(loc='best')
#plt.tight_layout()
#plt.savefig('/home/kharesp/log/rates.pdf')
#plt.close()

#plot latencies
n_groups=len(publishers)

fig,ax = plt.subplots()
index= np.arange(n_groups)
bar_width= .2
opacity= .4
  
l= plt.bar(index,latency,bar_width,alpha=opacity,color='b',label='latency')
l_to_eb= plt.bar(index+ bar_width, latency_to_eb,bar_width,alpha=opacity,color='g',label='latency to eb')
l_from_eb= plt.bar(index+ 2*bar_width,latency_from_eb, bar_width,alpha=opacity,color='r', label='latency from eb')

autolabel(l)
autolabel(l_to_eb)
autolabel(l_from_eb)
  
plt.xlabel('subscribers')
plt.ylabel('latency(ms)')
plt.xticks(index+bar_width/2,publishers)
plt.legend(loc='best')
plt.tight_layout()
plt.savefig('/home/kharesp/log/latency.pdf')
plt.close()
