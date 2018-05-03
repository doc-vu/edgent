import threading,time

def fun():
  print('hello')

t=threading.Timer(1,fun)
t.start()


while True:
  time.sleep(5)
