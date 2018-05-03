import threading,time,sys,os,signal,argparse
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import metadata,send_mail
from kazoo.client import KazooClient
from kazoo.recipe.watchers import DataWatch
from kazoo.protocol.states import EventType

class Track(object):
  def __init__(self,zk_connector):
    self.start_ts=-1
    self.runid=0
    self.execution_failed=False
    self.zk=KazooClient(hosts=zk_connector)
    self.zk.start()

  def install_listener(self):
    def listener(data,stat,event):
      try:
        print(data)
        if (event and event.type==EventType.CHANGED):
          curr_ts=int(time.time())
          if (self.start_ts > 0):
            elapsed_time= (curr_ts-self.start_ts)/(60.0)
            #print('Previous experiment run took:%f minutes\n'%(elapsed_time/(1000*60.0)))
            print(str(elapsed_time))
          self.start_ts=curr_ts
      except Exception as e:
        print(e)
    DataWatch(client=self.zk,path='/runid',func=listener,send_event=True)

  def start_update_timer(self):
    self.status_update=threading.Timer(600,self.update)
    self.status_update.start()

  def start_periodic_check_timer(self):
    self.periodic_check=threading.Timer(1200,self.check)
    self.periodic_check.start()

  def start(self):  
    self.install_listener()
    self.start_periodic_check_timer()
    self.start_update_timer()

  def stop(self):
    self.periodic_check.cancel()
    self.status_update.cancel()
    self.zk.stop()

  def check(self):
    value,info=self.zk.get('/runid')
    if(int(value)==self.runid):
      print('Test Execution has failed')
      self.execution_failed=True
      send_mail.Email().send(['kharesp28@gmail.com'],
        'Execution Failure','Check execution')
    else:
      self.runid=int(value)
      print('Updated run-id:%d'%(self.runid))
      self.start_periodic_check_timer()
  
  def update(self):
    value,info=self.zk.get('/runid')
    print('Sending status update for runid:%s'%(value))
    send_mail.Email().send(['kharesp28@gmail.com'],\
      'Update','run-id:%s'%(value))
    if not self.execution_failed:
      self.start_update_timer()


if __name__=="__main__":
  parser=argparse.ArgumentParser(description='script for monitoring test execution ')
  parser.add_argument('-zk_connector',required=True)
  args=parser.parse_args()

  tracker=Track(args.zk_connector)
  tracker.start()
  def signal_handler(signal,frame):
    tracker.stop()
    sys.exit(0)
  signal.signal(signal.SIGINT, signal_handler)
  while True:
    time.sleep(10)
