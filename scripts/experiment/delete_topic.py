from kazoo.client import KazooClient

zk=KazooClient(hosts='129.59.107.80:2181,129.59.107.97:2181,129.59.107.148:2181')
zk.start()
zk.delete('/eb/EB-30-10.20.30.2-0/t1',recursive=True)
zk.stop()
