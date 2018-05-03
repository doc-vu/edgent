import zmq,json

context= zmq.Context(1)

listener= context.socket(zmq.PULL)
listener.bind('tcp://*:1025')

pub=context.socket(zmq.PUB)
pub.bind('tcp://*:1026')

test_data={
  'command': 'start'
  'endpoint': 'pub',
  'zk_connector':'129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181',
  'experiment_type':'test',
  'topic_descriptions':{'isisws': 't1:10:10:100:4000:1000'}}
while True:
  msg= listener.recv()
  pub.send(msg)
