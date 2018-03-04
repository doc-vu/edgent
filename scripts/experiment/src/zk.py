from kazoo.client import KazooClient
from kazoo.recipe.barrier import Barrier
from kazoo.recipe.watchers import ChildrenWatch
from kazoo.protocol.states import EventType
import metadata,conf

class Zk(object):
  def __init__(self,exp_type,run_id,conf):
    self.exp_type=exp_type
    self.experiment_path='%s/%s'%(metadata.experiment_path,exp_type)
    self.run_id=run_id
    self.conf=conf
    self._zk=KazooClient(hosts=metadata.public_zk)
    self._zk.start()

  def clean(self):
   #delete zk /experiment path
   if self._zk.exists(self.experiment_path):
     self._zk.delete(self.experiment_path,recursive=True)

  def stop(self):
    self._zk.stop()


  def setup(self):
    self.create_paths() 
    self.install_watches()

  def wait(self,barrier):
    if (barrier=='subscriber'):
      self.sub_barrier.wait()
    elif(barrier=='finished'):
      self.finished_barrier.wait()
    elif(barrier=='logs'):
      self.logs_barrier.wait()
    else:
      print('Invalid barrier name')

  def create_paths(self):
    #create zk path for subscribers
    sub_path='%s/sub'%(self.experiment_path)
    self._zk.ensure_path(sub_path) 
    
    #create zk path for publishers
    pub_path='%s/pub'%(self.experiment_path)
    self._zk.ensure_path(pub_path) 

    #create zk path for monitors
    monitor_path='%s/monitor'%(self.experiment_path)
    self._zk.ensure_path(monitor_path)

    #create zk path to track when all data is sent
    data_sent_path='%s/sent'%(self.experiment_path)
    self._zk.ensure_path(data_sent_path)

    #create barrier paths 
    sub_barrier_path='%s/barriers/sub'%(self.experiment_path)
    if (self.conf.no_subscribers>0):
      self._zk.ensure_path(sub_barrier_path)
    pub_barrier_path='%s/barriers/pub'%(self.experiment_path)
    self._zk.ensure_path(pub_barrier_path)
    data_sent_barrier_path='%s/barriers/sent'%(self.experiment_path)
    self._zk.ensure_path(data_sent_barrier_path)
    finished_barrier_path='%s/barriers/finished'%(self.experiment_path)
    self._zk.ensure_path(finished_barrier_path)
    logs_barrier_path='%s/barriers/logs'%(self.experiment_path)
    self._zk.ensure_path(logs_barrier_path)

    #create barriers
    self.sub_barrier=Barrier(client=self._zk,path=sub_barrier_path)
    self.pub_barrier=Barrier(client=self._zk,path=pub_barrier_path)
    self.data_sent_barrier=Barrier(client=self._zk,\
      path=data_sent_barrier_path)
    self.finished_barrier=Barrier(client=self._zk,path=finished_barrier_path)
    self.logs_barrier=Barrier(client=self._zk,path=logs_barrier_path)

  def install_watches(self):
    #listener callback to track joined publishers and subscribers in all regions
    def _joined_endpoint_listener(children,event):
      if event and event.type==EventType.CHILD:
        if 'sub' in event.path :
          if (len(children)==self.conf.no_subscribers):
            print("All subscribers have joined. Opening subscriber barrier\n")
            self.sub_barrier.remove()
          elif (len(children)==0):
            print("All subscribers have left. Opening finished barrier\n")
            self.finished_barrier.remove()
        elif 'pub' in event.path: 
          if (len(children)==self.conf.no_publishers):
            print("All publishers have joined. Opening publisher barrier\n")
            self.pub_barrier.remove()
            return False
        elif 'monitor' in event.path:
          if (len(children)==0):
            print('All monitors have exited. Opening logs barrier')
            self.logs_barrier.remove()
            return False
        elif 'sent' in event.path: 
          if (len(children)==self.conf.no_publishers):
            print("All publishers have sent their data. Opening data sent barrier\n")
            self.data_sent_barrier.remove()
            return False

    #watch to open logs barrier when all Monitors have exited
    ChildrenWatch(client=self._zk,\
      path='%s/monitor'%(self.experiment_path),\
      func=_joined_endpoint_listener,send_event=True)

    #watch for tracking joined subscribers
    ChildrenWatch(client=self._zk,\
      path='%s/sub'%(self.experiment_path),\
      func=_joined_endpoint_listener,send_event=True)

    #watch for tracking joined publishers
    ChildrenWatch(client=self._zk,\
      path='%s/pub'%(self.experiment_path),\
      func=_joined_endpoint_listener,send_event=True)

    #watch for tracking when all data is sent by publishers 
    ChildrenWatch(client=self._zk,\
      path='%s/sent'%(self.experiment_path),\
      func=_joined_endpoint_listener,send_event=True)
