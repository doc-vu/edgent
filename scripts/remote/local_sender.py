import zmq,json,time

context= zmq.Context(1)

sender= context.socket(zmq.PUSH)
sender.connect('tcp://129.59.104.151:1025')
time.sleep(5)

test_data={
  'command': 'start',
  'endpoint': 'pub',
  'zk_connector':'129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181',
  'experiment_type':'test',
  'topic_descriptions':{'node5': 't1:1:1:120:4000:1000'}}
while True:
  sender.send(json.dumps(test_data))
  time.sleep(5)
  print('sent message')
  break
