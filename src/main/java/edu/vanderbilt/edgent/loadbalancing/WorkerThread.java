package edu.vanderbilt.edgent.loadbalancing;

import java.util.List;
import org.apache.curator.framework.CuratorFramework;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

public class WorkerThread implements Runnable {
	//ZContext
	private ZContext context;
	//ZMQ Socket to receive topic creation request
	private ZMQ.Socket socket;
	//curator client to talk to ZK
	private CuratorFramework client;

	private String workerId;
	private Logger logger;

	public WorkerThread(ZContext context,CuratorFramework client){
		this.context=context;
		this.client=client;
		logger=LogManager.getLogger(this.getClass().getSimpleName());
	}

	@Override
	public void run() {
		workerId= Thread.currentThread().getName();
		logger.debug("WorkerThread:{} started",workerId);
	
		//Create socket to receive topic creation requests from LB 
		socket=context.createSocket(ZMQ.PULL);
		socket.connect(LoadBalancer.INPROC_CONNECTOR);
	
		//listener loop
		while(!Thread.currentThread().isInterrupted()){
			try{
				String topic=socket.recvStr(0);
				logger.debug("WorkerThread:{} received topic creation request for topic:{}",
						workerId,topic);
				create(topic);
			}catch(ZMQException e){
				logger.error("WorkerThread:{} caught ZMQexception",workerId);
				break;
			}
				
		}
		//clean up before exiting
		socket.close();
		context.destroy();
		logger.error("WorkerThread:{} exited",workerId);
	}
	
	private void create(String topic){
		try{
			//retrieve list of EBs
			List<String> ebs= client.getChildren().forPath("/eb");
			
			//select EB to host the new topic
			String selectedEb= selectEb(ebs);
			logger.debug("WorkerThread:{} selected EB:{} for hosting topic:{}",workerId,
					selectedEb,topic);
			
			//create topic znode under selected EB
			client.create().forPath(String.format("/eb/%s/%s/pub",selectedEb,topic));
			client.create().forPath(String.format("/eb/%s/%s/sub",selectedEb,topic));
			logger.debug("WorkerThread:{} created topic:{} znode under EB:{}",workerId,
					topic,selectedEb);
		}catch(Exception e){
			
		}
	}

	/*
	 * TODO: Currently, an EB is randomly selected to host 
	 * the new topic. 
	 */
	private String selectEb(List<String> ebs){
		int selectedBroker = (int) (Math.random() * ebs.size());
		return ebs.get(selectedBroker);
	}
	

}
