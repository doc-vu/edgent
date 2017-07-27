"""
Script to parse the test configuration
"""
class Conf(object):
  def __init__(self,conf_file):
    self.conf_file=conf_file
    self.parse()
    
  def parse(self):
    with open(self.conf_file) as f:
      for line in f:
        #run_id
        if line.startswith('run_id'):
          self.run_id=line.rstrip().partition(':')[2]
        #rbs
        elif line.startswith('rbs'):
          self.rbs=line.rstrip().partition(':')[2].split(',')
        #ebs
        elif line.startswith('ebs'):
          self.ebs=line.rstrip().partition(':')[2].split(',')
        #client host machines
        elif line.startswith('clients'):
          self.clients=line.rstrip().partition(':')[2].split(',')
        #topics
        elif line.startswith('topics'):
          self.topics=line.rstrip().partition(':')[2].split(',')
          self.no_topics=len(self.topics)
        #number of subscribers
        elif line.startswith('no_subs'):
          self.no_subscribers=int(line.rstrip().partition(':')[2])
        #number of publishers
        elif line.startswith('no_pubs'):
          self.no_publishers=int(line.rstrip().partition(':')[2])
        #subscriber distribution
        elif line.startswith('sub_distribution'):
          self.subscribers={}
          for sub_description in line.rstrip().partition(':')[2].split(','):
            host,topic,num_sub= sub_description.split(':')
            if host in self.subscribers:
              self.subscribers[host].update({topic: num_sub})
            else:
              self.subscribers[host]={topic: num_sub}
        #publisher distribution
        elif line.startswith('pub_distribution'):
          self.publishers={}  
          for pub_description in line.rstrip().partition(':')[2].split(','):
            host,topic,num_pub= pub_description.split(':')
            if host in self.publishers:
              self.publishers[host].update({topic: num_pub})
            else:
              self.publishers[host]={topic: num_pub}
        #publisher sample count
        elif line.startswith('pub_sample_count'):
          self.pub_sample_count=int(line.rstrip().partition(':')[2])
        #subscriber sample count
        elif line.startswith('sub_sample_count'):
          self.sub_sample_count=int(line.rstrip().partition(':')[2])
        #publisher sleep interval
        elif line.startswith('sleep_interval_milisec'):
          self.sleep_interval_milisec=int(line.rstrip().partition(':')[2])
        else:
          print('invalid line:%s'%(line))
    
    #eb and rb brokers
    self.brokers= ','.join(self.ebs)+','+','.join(self.rbs)
    #all host machines
    self.hosts=self.brokers+','+','.join(self.clients)
    
    #region to number of client subscriber machines map
    self.region_clientsubscribers_map={}
    for client in self.subscribers.keys():
      region=client[3:client.index('-')]
      if region in self.region_clientsubscribers_map:
        self.region_clientsubscribers_map[region]+=1
      else:
        self.region_clientsubscribers_map[region]=1

    #region to number of client publisher machines map
    self.region_clientpublishers_map={}
    for client in self.publishers.keys():
      region=client[3:client.index('-')]
      if region in self.region_clientpublishers_map:
        self.region_clientpublishers_map[region]+=1
      else:
        self.region_clientpublishers_map[region]=1
      
    #client machine to number of hosted subscribers map  
    self.client_numSubscribers={ client: sum([int(num_sub) for num_sub in topics.values()]) for client,topics in self.subscribers.items() }
    #client machine to number of hosted publishers map
    self.client_numPublishers={ client: sum([int(num_pub) for num_pub in topics.values()]) for client,topics in self.publishers.items() }

if __name__=="__main__":
  Conf('conf/conf.csv')
