import matplotlib.pyplot as plt
import numpy as np

publishers=[5,10,15,20,25,30,35,40,45,50,55,60]

pub_node={}
eb_node={}
sub_node={}

pub_node['mem']=[3.917101,3.896893,3.904423,3.885918,3.907354,3.911923,3.942998,3.907945,3.899694,3.924277,3.921402,3.916385]
pub_node['iowait']=[0.181,0.158416,0.1909,0.2728,0.2225,0.200198,0.16202,0.216337,0.158081,0.162059,0.158283,0.2566]
pub_node['cpu']=[35.0951,35.197228,34.7711,35.6042,33.9827,34.57396,35.666061,35.294356,35.805152,34.133725,35.126869,35.4149]
pub_node['nw']=[740.954444,740.9322,740.319394,740.139596,737.156263,740.8142,748.297959,733.5864,747.801122,732.728713,745.461837,741.000808]

eb_node['mem']=[0.535846,0.542128,0.536665,0.53498,0.529916,0.53588,0.534782,0.529857,0.526403,0.537216,0.530725,0.530975]
eb_node['iowait']=[0.1292,0.1844,0.138081,0.1535,0.1356,0.1076,0.227129,0.13402,0.1183,0.080097,0.1816,0.115446]
eb_node['cpu']=[16.4352,20.2688,24.966061,27.2183,29.6964,33.1755,36.360396,38.81402,41.9991,43.658738,46.6302,49.05901]
eb_node['nw']=[4041.61798,7268.883131,10604.444694,13621.670404,16675.884747,19603.479293,22581.9948,25288.048119,28499.600101,30712.309608,34058.787071,36615.0459]

sub_node['mem']=[0.96732,1.004545,1.02710166667,1.03423275,1.036603,1.0283165,1.03018028571,1.03708425,1.04512955556,1.0387849,1.02938690909,1.03147675]
sub_node['iowait']=[0.2867,0.1829575,0.139066666667,0.276825,0.3296386,0.287216666667,0.33733,0.217814625,0.271742777778,0.2356108,0.295691090909,0.4514475]
sub_node['cpu']=[31.0369,30.257283,30.9363,30.8897,30.5400262,29.8753783333,29.8349638571,29.292350875,29.4620413333,28.717694,28.7725307273,28.1562994167]
sub_node['nw']=[3297.348283,3281.621209,3256.386229,3206.89924225,3189.90881,3155.80905167,3141.81843357,3085.11871238,3100.606521,3014.0434445,3041.84124609,2970.27382467]

n_groups=len(publishers)

resources=['cpu','iowait','mem','nw']
for resource in resources:
  fig,ax = plt.subplots()
  index= np.arange(n_groups)
  bar_width= .2
  opacity= .4
  
  pub= plt.bar(index,pub_node[resource],bar_width,alpha=opacity,color='b',label='pub node')
  eb= plt.bar(index+ bar_width, eb_node[resource],bar_width,alpha=opacity,color='g',label='eb node')
  sub= plt.bar(index+ 2*bar_width, sub_node[resource],bar_width,alpha=opacity,color='r', label='sub node')
  
  def autolabel(rects):
      for rect in rects:
          height = rect.get_height()
          ax.text(rect.get_x() + rect.get_width()/2., 1.05*height,
                  '%d' % int(height),
                  ha='center', va='bottom')
 
  if(not resource == 'nw'): 
    autolabel(pub)
    autolabel(eb)
    autolabel(sub)
  
  plt.xlabel('subscribers')
  plt.ylabel('%s'%resource)
  plt.xticks(index+bar_width/2,publishers)
  plt.legend(loc='best')
  plt.tight_layout()
  plt.savefig('/home/kharesp/log/%s.pdf'%resource)
