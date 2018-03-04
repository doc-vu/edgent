import numpy as np
"""
topic's load will be described by a dictionary: 
A single publisher can publish either at rate1=1 msg/sec, rate2=5 msg/sec or rate3=10 msg/sec
topic_description={
  name: 't1'
  aggregate_rate: [10,30,50,70,90]
  publisher_distribution: [ #pub:rate1,#pub:rate2,#pub:rate3]
  subscription_size= range(10,100,10)
  #publishers: number of connected publishers as per publisher_distribution
}
"""
#supported publication rates per publisher
per_publisher_publication_rates=[1,5,10]
#supported aggregate publication rates per topic
aggregate_publication_rates=[10,30,50,70,90]
#publisher distributions
publisher_distributions={
  'r10':['10:1','5:1,1:5','2:5','1:10'],
  'r30':['30:1','20:1,2:5','10:1,2:5,1:10','4:5,1:10','3:10'],
  'r50':['50:1','30:1,2:5,1:10','20:1,6:5','10:1,6:5,1:10','5:10'],
  'r70':['70:1','50:1,2:5,1:10','30:1,2:5,3:10','20:1,5:10','7:10'],
  'r90':['90:1','60:1,6:5','40:1,2:5,4:10','20:1,4:5,5:10','9:10'],
}
subscription_size=range(10,110,10)
print(subscription_size)
