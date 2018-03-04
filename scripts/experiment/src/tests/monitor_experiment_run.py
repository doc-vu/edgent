import threading,time,sys,os,signal
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import metadata,send_mail
from kazoo.client import KazooClient

class Track(object):
  def __init__(self):
    self.runid=0
    self.execution_failed=False
    self.zk=KazooClient(hosts=metadata.public_zk)
    self.zk.start()

  def start_update_timer(self):
    self.status_update=threading.Timer(3600,self.update)
    self.status_update.start()

  def start_periodic_check_timer(self):
    self.periodic_check=threading.Timer(1200,self.check)
    self.periodic_check.start()

  def start(self):  
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
  tracker=Track()
  tracker.start()
  def signal_handler(signal,frame):
    tracker.stop()
    sys.exit(0)
  signal.signal(signal.SIGINT, signal_handler)
  while True:
    time.sleep(10)
