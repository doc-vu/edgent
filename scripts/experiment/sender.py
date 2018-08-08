import zmq,time,json

context=zmq.Context(1)
socket=context.socket(zmq.PUSH)
socket.set_hwm(0)
socket.connect('tcp://129.59.104.151:1025')
time.sleep(2)


kill_command={'command': 'kill','global':False, 'target':'node4'}

print('Sending command')
socket.send_string(json.dumps(kill_command))
print('Sent command')
time.sleep(2)


#socket.setsockopt(zmq.LINGER,0)
socket.close()
context.term()
print('exiting')
