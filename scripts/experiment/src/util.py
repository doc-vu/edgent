from kazoo.client import KazooClient
import multiprocessing,metadata,subprocess,os
from kazoo.recipe.barrier import Barrier
from kazoo.recipe.watchers import ChildrenWatch
from kazoo.protocol.states import EventType
from kazoo.retry import RetryFailedError
from kazoo.exceptions import KazooException
from functools import reduce
import zmq,json,time,metadata

class Coordinate(object):
  def __init__(self,exp_type,run_id,
    subscribers,publishers,brokers,log_dir,wait_flag,zk_connector,fe_address):
    self.exp_type=exp_type
    self.run_id=run_id
    self.exp_path='/experiment/%s'%(exp_type)
    self.subscribers=subscribers
    self.publishers=publishers
    self.brokers=brokers
    self.log_dir=log_dir
    self.wait_flag=wait_flag
    self.zk_connector=zk_connector
    self.fe_address=fe_address
    
    self.no_subscribers=reduce(lambda x,y:x+y,\
      [ int(t.split(':')[1]) for t in reduce(lambda x,y:x+y,subscribers.values())])
    self.no_publishers=reduce(lambda x,y:x+y,\
      [ int(t.split(':')[1]) for t in reduce(lambda x,y:x+y,publishers.values())])

    self.context=zmq.Context(1)    
    self.socket=self.context.socket(zmq.PUSH)
    self.socket.connect('tcp://129.59.104.151:1025')
    #allow connection to finish
    time.sleep(2)

    #setup zk coordination
    self.setup()

  def setup(self):
    self._zk=KazooClient(hosts=self.zk_connector)
    self._zk.start()
    try:
      #clean exp_path
      print('Cleaning Experiment path')
      self._zk.retry(self.clean)
      #create necessary zk paths
      self._zk.retry(self.create_paths)
      print('Created paths')
      print(self.run_id)
      self._zk.retry(self.create_barriers)
      self._zk.retry(self.install_watches)
      self._zk.set('/runid',bytes('%d'%(self.run_id),'utf-8'))
    except (KazooException, RetryFailedError) as e:
      print(str(e))
      self.stop()

  def run(self):
    #start endpoints
    self.start()

    if self.wait_flag:
      #wait for test to finish if flag it set
      self.wait()
      print('Test has ended')

      #kill endpoints
      print('Killing publisher processes')
      kill('edgent.endpoints',','.join(self.publishers.keys()))

      #collect logs
      print('Collecting logs')
      client_and_broker_machines=[]
      for x in self.brokers:
        client_and_broker_machines.append(x)
      for x in self.subscribers.keys():
        client_and_broker_machines.append(x)
      for x in self.publishers.keys():
        client_and_broker_machines.append(x)
      collect_logs(self.run_id,self.log_dir,','.join(client_and_broker_machines))
      
      #create finished node
      self._zk.retry(self.create_finished_node)

    #stop zk
    self.stop()

  def start(self): 
    try:
      print('Starting subscribers')
      start_subscribers(self.subscribers,self.run_id,self.exp_type,self.fe_address)
      #start_subscribers2(self.socket,self.subscribers,self.run_id,self.exp_type)
      print('Waiting for all subscribers to join')
      self._zk.retry(self.sub_barrier.wait)
      print('Starting publishers')
      start_publishers(self.publishers,self.exp_type,self.zk_connector,self.fe_address)
      #start_publishers2(self.socket,self.publishers,self.exp_type)
      print('Waiting for all publishers to join')
      self._zk.retry(self.pub_barrier.wait)
    except (KazooException,RetryFailedError) as e:
      #print(str(e))
      self.stop()

  def wait(self):
    try:
      print('Waiting for test to finish execution')
      self._zk.retry(self.finished_barrier.wait)
      self._zk.retry(self.logs_barrier.wait)
    except (KazooException, RetryFailedError) as e:
      print(str(e))
      self.stop()

  def kill(self):
    msg={'command':'kill'}
    self.socket.send(json.dumps(msg)) 

  def stop(self):
    self.socket.close()
    self.context.destroy()
    self._zk.stop()

  def clean(self):
    if self._zk.exists(self.exp_path):
      self._zk.delete(self.exp_path,recursive=True)

  def create_finished_node(self):
    self._zk.ensure_path('%s/finished'%(self.exp_path))
  
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

def start_monitors(run_id,experiment_type,pub_node,hosts,zk_connector):
  command_string='cd %s && ansible-playbook playbooks/experiment/monitor.yml -f 16 \
    --extra-vars="run_id=%s zk_connector=%s\
    pub_node=%d experiment_type=%s core_count=%d" --limit %s'%\
   (metadata.ansible,run_id,zk_connector,pub_node,experiment_type,\
   metadata.restricted_core_count,hosts)
  subprocess.check_call(['bash','-c',command_string])

def start_subscribers2(socket,subscribers,run_id,experiment_type):
  msg={'command':'start',
    'endpoint': 'sub',
    'experiment_type': experiment_type,
    'run_id': run_id,
    'topic_descriptions': subscribers,
    'out_dir': metadata.remote_log_dir,
  }
  socket.send(json.dumps(msg))

def start_subscribers(subscribers,run_id,experiment_type,fe_address):
  parallelism=16
  sorted_hosts=sorted(subscribers.keys())
  num=len(sorted_hosts)//parallelism
  rem=len(sorted_hosts)%parallelism
  
  def launch_sub(host,topic_descriptions,run_id,experiment_type):
    command_string='cd %s && ansible-playbook playbooks/experiment/subscriber2.yml  --limit %s\
    --extra-vars="topic_descriptions=%s run_id=%s experiment_type=%s fe_address=%s"'%\
    (metadata.ansible,host,','.join(topic_descriptions),run_id,experiment_type,fe_address)
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

def start_publishers2(socket,publishers,experiment_type):
  msg={'command':'start',
    'endpoint': 'pub',
    'zk_connector': metadata.public_zk,
    'experiment_type': experiment_type,
    'topic_descriptions': publishers,
  }
  socket.send(json.dumps(msg))
  
def start_publishers(publishers,experiment_type,zk_connector,fe_address):
  parallelism=16
  sorted_hosts=sorted(publishers.keys())
  num=len(sorted_hosts)//parallelism
  rem=len(sorted_hosts)%parallelism
  
  def launch_pub(host,topic_descriptions,experiment_type):
    command_string='cd %s && ansible-playbook playbooks/experiment/publisher2.yml  --limit %s\
    --extra-vars="topic_descriptions=%s zk_connector=%s experiment_type=%s fe_address=%s"'%\
    (metadata.ansible,host,','.join(topic_descriptions),zk_connector,experiment_type,fe_address)
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


def start_eb(brokers,zk_connector):
  command_string='cd %s && ansible-playbook playbooks/experiment/eb.yml  --limit %s\
    --extra-vars="zk_connector=%s io_threads=%d"'%\
    (metadata.ansible,brokers,zk_connector,metadata.io_threads)
  subprocess.check_call(['bash','-c',command_string])
