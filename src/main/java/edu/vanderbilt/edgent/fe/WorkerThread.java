package edu.vanderbilt.edgent.fe;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMQ.Socket;

public class WorkerThread implements Runnable {
	private ZContext context;
	private CuratorFramework client;

	public WorkerThread(ZContext context,CuratorFramework client){
		this.context=context;
		this.client=client;
	}

	@Override
	public void run() {
		ZMQ.Socket socket= context.createSocket(ZMQ.REP);
		socket.connect(Frontend.INPROC_CONNECTOR);

		while(!Thread.currentThread().isInterrupted()){
			try{
				String req=socket.recvStr(0);
				String[] args=req.split(",");
				if (args[0].equals(Frontend.CONNECTION_REQUEST)) {
					String topic = args[1];
					String endpointType = args[2];
					String ip = args[3];
					connect(socket, topic, endpointType, ip);

				} 
				//process DISCONNECTION_REQUEST
				else if (args[0].equals(Frontend.DISCONNECTION_REQUEST)) {
					String znode = args[1];
					socket.send("");
				} else {
					socket.send("");
				}
			}catch(ZMQException e){
				System.out.println("Exception caught");
				break;
			}
		
		}
		socket.close();
		context.destroy();
		System.out.println("Exited!");
	}
	
	private void connect(Socket socket, String topic, String endpointType, String ip) {
		try {
			// check if topic of interest exists
			Stat res= client.checkExists().forPath(String.format("/topics/%s", topic));
			
			List<String> brokers = client.getChildren().forPath(String.format("/topics/%s", topic));

			// TODO: Currently, we are selecting any random broker to connect
			// to. This can be changed, so that we connect to the least loaded
			// broker.
			int selectedBroker = (int) (Math.random() * brokers.size());

			// get selected broker's znode data
			String eb = new String(
					client.getData().forPath(String.format("/topics/%s/%s", topic, brokers.get(selectedBroker))));

			// create znode for this new endpoint which has joined the system
			String znodePath = client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL)
					.forPath(String.format("/eb/%s/%s/%s/p", brokers.get(selectedBroker), topic, endpointType),
							ip.getBytes());

			// return path of znode and EB to the endpoint
			socket.send(String.format("%s;%s", eb, znodePath), 0);

		} catch (NoNodeException e) {
			socket.send("Does not exist");
			// topic is new to system

		} catch (Exception e) {
			e.printStackTrace();
			socket.send("null");
		}
	}
	

}
