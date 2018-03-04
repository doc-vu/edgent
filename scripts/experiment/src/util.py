from kazoo.client import KazooClient
import multiprocessing,metadata,subprocess,os
from kazoo.recipe.barrier import Barrier
from kazoo.recipe.watchers import ChildrenWatch
from kazoo.protocol.states import EventType
from kazoo.retry import RetryFailedError
from kazoo.exceptions import KazooException

class Coordinate(object):
  def __init__(self,exp_type,run_id,subscribers,publishers):
    self.exp_type=exp_type
    self.run_id=run_id
    self.exp_path='/experiment/%s'%(exp_type)
    self.subscribers=subscribers
    self.publishers=publishers
    
    self.no_subscribers=reduce(lambda x,y:x+y,\
      [ int(t.split(':')[1]) for t in reduce(lambda x,y:x+y,subscribers.values())])
    self.no_publishers=reduce(lambda x,y:x+y,\
      [ int(t.split(':')[1]) for t in reduce(lambda x,y:x+y,publishers.values())])
    
    #setup zk coordination
    self.setup()

  def setup(self):
    self._zk=KazooClient(hosts=metadata.public_zk)
    self._zk.start()
    try:
      #clean exp_path
      self._zk.retry(self.clean)
      #create necessary zk paths
      self._zk.retry(self.create_paths)
      self._zk.retry(self.create_barriers)
      self._zk.retry(self.install_watches)
    except (KazooException, RetryFailedError) as e:
      print(str(e))
      self.stop()

  def start(self): 
    try:
      start_subscribers(self.subscribers,self.run_id,self.exp_type)
      self._zk.retry(self.sub_barrier.wait)
      start_publishers(self.publishers,self.exp_type)
      self._zk.retry(self.pub_barrier.wait)
    except (KazooException,RetryFailedError) as e:
      print(str(e))
      self.stop()

  def wait(self):
    try:
      self._zk.retry(self.finished_barrier.wait)
      self._zk.retry(self.logs_barrier.wait)
    except (KazooException, RetryFailedError) as e:
      print(str(e))
      self.stop()

  def stop(self):
    self._zk.stop()

  def clean(self):
    if self._zk.exists(self.exp_path):
      self._zk.delete(self.exp_path,recursive=True)
  
  def create_paths(self):
    #joined endpoint paths
    self._zk.ensure_path('%s/sub'%(self.exp_path))
    self._zk.ensure_path('%s/pub'%(self.exp_path))
    self._zk.ensure_path('%s/sent'%(self.exp_path))
    self._zk.ensure_path('%s/monitor'%(self.exp_path))
    #barrier paths
    self._zk.ensure_path('%s/barriers/sub'%(self.exp_path))
    self._zk.ensure_path('%s/barriers/pub'%(self.exp_path))
    self._zk.ensure_path('%s/barriers/finished'%(self.exp_path))
    self._zk.ensure_path('%s/barriers/logs'%(self.exp_path))

  def create_barriers(self):
    self.sub_barrier=Barrier(client=self._zk,path='%s/barriers/sub'%(self.exp_path))
    self.pub_barrier=Barrier(client=self._zk,path='%s/barriers/pub'%(self.exp_path))
    self.finished_barrier=Barrier(client=self._zk,path='%s/barriers/finished'%(self.exp_path))
    self.logs_barrier=Barrier(client=self._zk,path='%s/barriers/logs'%(self.exp_path))

  def install_watches(self):
    def _joined_endpoint_listener(children,event):
      if event and event.type==EventType.CHILD:
        if 'sub' in event.path :
          if (len(children)==self.no_subscribers):
            print("All subscribers have joined. Opening subscriber barrier\n")
            self.sub_barrier.remove()
          elif (len(children)==0):
            print("All subscribers have left. Opening finished barrier\n")
            self.finished_barrier.remove()
            return False
        elif 'pub' in event.path: 
          if (len(children)==self.no_publishers):
            print("All publishers have joined. Opening publisher barrier\n")
            self.pub_barrier.remove()
            return False
        elif 'monitor' in event.path:
          if (len(children)==0):
            print('All monitors have exited. Opening logs barrier')
            self.logs_barrier.remove()
            return False

    #watch for tracking joined subscribers
    ChildrenWatch(client=self._zk,\
      path='%s/sub'%(self.exp_path),\
      func=_joined_endpoint_listener,send_event=True)

    #watch for tracking joined publishers
    ChildrenWatch(client=self._zk,\
      path='%s/pub'%(self.exp_path),\
      func=_joined_endpoint_listener,send_event=True)

    #watch to open logs barrier when all Monitors have exited
    ChildrenWatch(client=self._zk,\
      path='%s/monitor'%(self.exp_path),\
      func=_joined_endpoint_listener,send_event=True)

def kill(pattern,hosts=None):
  if hosts:
    command_string='cd %s && ansible-playbook playbooks/util/kill.yml -f 16 --limit %s\
      --extra-vars="pattern=%s"'%(metadata.ansible,hosts,pattern)
    subprocess.check_call(['bash','-c', command_string])
  else:
    command_string='cd %s && ansible-playbook playbooks/util/kill.yml -f 16 \
      --extra-vars="pattern=%s"'%(metadata.ansible,pattern)
    subprocess.check_call(['bash','-c', command_string])

def clean_logs(hosts=None):   
  if hosts:
    command_string='cd %s && ansible-playbook playbooks/util/clean.yml -f 16\
      --extra-vars="dir=/home/ubuntu/log/" --limit %s'\
      %(metadata.ansible,hosts)
    subprocess.check_call(['bash','-c',command_string])
  else:
    command_string='cd %s && ansible-playbook playbooks/util/clean.yml -f 16\
      --extra-vars="dir=/home/ubuntu/log/"'%(metadata.ansible)
    subprocess.check_call(['bash','-c',command_string])

def clean_memory(hosts=None):
  if hosts:
    command_string='cd %s && ansible-playbook playbooks/experiment/clean_memory.yml  -f 16 --limit %s'%(metadata.ansible,hosts)
    subprocess.check_call(['bash','-c',command_string])
  else:
    command_string='cd %s && ansible-playbook playbooks/experiment/clean_memory.yml  -f 16'%(metadata.ansible)
    subprocess.check_call(['bash','-c',command_string])

def start_monitors(run_id,experiment_type,pub_node,hosts):
  command_string='cd %s && ansible-playbook playbooks/experiment/monitor.yml -f 16 \
    --extra-vars="run_id=%s zk_connector=%s\
    pub_node=%d experiment_type=%s core_count=%d" --limit %s'%\
   (metadata.ansible,run_id,metadata.public_zk,pub_node,experiment_type,\
   metadata.restricted_core_count,hosts)
  subprocess.check_call(['bash','-c',command_string])

def start_subscribers(subscribers,run_id,experiment_type):
  parallelism=16
  sorted_hosts=sorted(subscribers.keys())
  num=len(sorted_hosts)//parallelism
  rem=len(sorted_hosts)%parallelism
  
  def launch_sub(host,topic_descriptions,run_id,experiment_type):
    command_string='cd %s && ansible-playbook playbooks/experiment/subscriber2.yml  --limit %s\
    --extra-vars="topic_descriptions=%s run_id=%s experiment_type=%s"'%\
    (metadata.ansible,host,','.join(topic_descriptions),run_id,experiment_type)
    subprocess.check_call(['bash','-c',command_string])
    
  for i in range(num):
    pairs={k:subscribers[k] for k in sorted_hosts[i*parallelism:i*parallelism+parallelism]} 
    processes=[multiprocessing.Process(target=launch_sub,args=(host,topic_descriptions,run_id,experiment_type)) for host,topic_descriptions in pairs.items()]
    for p in processes:
      p.start()
    for p in processes:
      p.join()
       
  pairs={k:subscribers[k] for k in sorted_hosts[num*parallelism:]} 
  processes=[multiprocessing.Process(target=launch_sub,args=(host,topic_descriptions,run_id,experiment_type)) for host,topic_descriptions in pairs.items()]
  for p in processes:
    p.start()
  for p in processes:
    p.join()

def start_publishers(publishers,experiment_type):
  parallelism=16
  sorted_hosts=sorted(publishers.keys())
  num=len(sorted_hosts)//parallelism
  rem=len(sorted_hosts)%parallelism
  
  def launch_pub(host,topic_descriptions,experiment_type):
    command_string='cd %s && ansible-playbook playbooks/experiment/publisher2.yml  --limit %s\
    --extra-vars="topic_descriptions=%s zk_connector=%s experiment_type=%s"'%\
    (metadata.ansible,host,','.join(topic_descriptions),metadata.public_zk,experiment_type)
    subprocess.check_call(['bash','-c',command_string])
    
  for i in range(num):
    pairs={k:publishers[k] for k in sorted_hosts[i*parallelism:i*parallelism+parallelism]} 
    processes=[multiprocessing.Process(target=launch_pub,args=(host,topic_descriptions,experiment_type)) for host,topic_descriptions in pairs.items()]
    for p in processes:
      p.start()
    for p in processes:
      p.join()
       
  pairs={k:publishers[k] for k in sorted_hosts[num*parallelism:]} 
  processes=[multiprocessing.Process(target=launch_pub,args=(host,topic_descriptions,experiment_type)) for host,topic_descriptions in pairs.items()]
  for p in processes:
    p.start()
  for p in processes:
    p.join()

def collect_logs(run_id,log_dir,hosts):
  #ensure local log directory exists
  dest_dir='%s'%(log_dir)
  if not os.path.exists(dest_dir):
    os.makedirs(dest_dir)

  #fetch logs from all hosts
  command_string='cd %s && ansible-playbook playbooks/util/copy.yml  --limit %s\
    --extra-vars="src_dir=%s dest_dir=%s"'%(metadata.ansible,
    hosts,'%s/%s'%(metadata.remote_log_dir,run_id), dest_dir)
  subprocess.check_call(['bash','-c',command_string])


def start_eb(brokers):
  command_string='cd %s && ansible-playbook playbooks/experiment/eb.yml  --limit %s\
    --extra-vars="zk_connector=%s io_threads=%d"'%\
    (metadata.ansible,','.join(brokers),metadata.public_zk,metadata.io_threads)
  subprocess.check_call(['bash','-c',command_string])
