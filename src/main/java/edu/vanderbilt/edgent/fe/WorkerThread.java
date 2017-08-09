package edu.vanderbilt.edgent.fe;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

public class WorkerThread implements Runnable {
	//zmq context and reply socket
	private ZContext context;
	private ZMQ.Socket socket;

	//Curator client for connecting to ZK 
	private CuratorFramework client;

	private Logger logger;
	private String workerId;
	
	public WorkerThread(ZContext context, String zkConnector){
		logger=  LogManager.getLogger(this.getClass().getSimpleName());

		this.context=context;
		client=CuratorFrameworkFactory.newClient(zkConnector,
				new ExponentialBackoffRetry(1000, 3));
		client.start();
	}

	@Override
	public void run() {
		//stash this worker thread's id
		workerId=Thread.currentThread().getName();
		logger.debug("WorkerThread:{} started",workerId);

		//create ZMQ Reply socket to receive, process and respond to incoming requests
		socket=context.createSocket(ZMQ.REP);
		socket.connect(Frontend.INPROC_CONNECTOR);

		//main worker loop
		while(!Thread.currentThread().isInterrupted()){
			try{
				String req=socket.recvStr();
				String[] args= req.split(",");
				if(args[0].equals(Frontend.CONNECTION_REQUEST))
				{
					String topic=args[1];
					String endpointType=args[2];
					String ip=args[3];
					logger.debug("WorkerThread:{} received {} request for topic:{} endpoint type:{} and ip:{}",
							workerId,Frontend.CONNECTION_REQUEST,topic,endpointType,ip);
					connect(topic,endpointType,ip);
					
				}else if(args[0].equals(Frontend.DISCONNECTION_REQUEST)){
					String znode=args[1];
					logger.debug("WorkerThread:{} received {} request for znode:{}",
							workerId,Frontend.DISCONNECTION_REQUEST,znode);
					diconnect(znode);
				}else{
					logger.error("WorkerThread:{} received bad request {}",workerId,args[0]);
					socket.send("");
				}

			}catch(ZMQException e){
				logger.error("WorkerThread:{} execution was interruped",workerId);
				break;
			}
		}

		//close ZMQ socket and context before exiting
		socket.close();
		context.destroy();
		//close Curator connection to ZK
		CloseableUtils.closeQuietly(client);
		logger.debug("WrokerThread:{} closed ZMQ socket. Exiting.",workerId);
	}

	public void connect(String topic,String endpointType,String ip){
		//check if topic of interest exists
		try {
			List<String> brokers= client.getChildren().forPath(String.format("/topics/%s", topic));
			for(String broker:brokers){
				System.out.println(broker);

			}
			socket.send("broker found");
			
		}catch(NoNodeException e){
			socket.send("Does not exist");
			//topic is new to system
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		//if this is a new topic, send request to LB to allocate topic
		//return EB connector and znode for this endpoint
	}
	
	public void diconnect(String znode){
		
	}

}
