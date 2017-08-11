package edu.vanderbilt.edgent.fe;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import edu.vanderbilt.edgent.loadbalancing.LoadBalancer;

public class WorkerThread implements Runnable {
	//Shadowed ZContext 
	private ZContext context;
	//ZMQ Sockets to receive requests from FE  
	//and send topic creation requests to LB 
	private ZMQ.Socket feSocket;  
	private ZMQ.Socket lbSocket; 

	//load balancer address
	private String lbAddress;
	
	//Curator client to talk to ZK
	private CuratorFramework client;

	//Worker Thread Id
	private String workerId;
	private Logger logger;

	public WorkerThread(ZContext context,
			CuratorFramework client,String lbAddress){
		logger=LogManager.getLogger(this.getClass().getSimpleName());
		this.context=context;
		this.client=client;
		this.lbAddress=lbAddress;
	}

	@Override
	public void run() {
		workerId=Thread.currentThread().getName();
		logger.debug("WorkerThread:{} started",workerId);

		//connect to FE to receive incoming requests
		feSocket= context.createSocket(ZMQ.REP);
		feSocket.connect(Frontend.INPROC_CONNECTOR);
	
		//connect to LB to send topic creation requests
		lbSocket=context.createSocket(ZMQ.PUSH);
		lbSocket.connect(String.format("tcp://%s:%d",
				lbAddress,LoadBalancer.LISTENER_PORT));

		
		//Listening loop
		while(!Thread.currentThread().isInterrupted()){
			try{
				String req=feSocket.recvStr(0);
				String[] args=req.split(",");

				//Process CONNECTION_REQUEST
				if (args[0].equals(Frontend.CONNECTION_REQUEST)) {
					String topic = args[1];
					String endpointType = args[2];
					String endpointAddress = args[3];

					logger.debug("WorkerThread:{} received request:{}"
							+ "for topic:{}, endpointType:{} and ip:{} ",workerId,
							Frontend.CONNECTION_REQUEST,topic,endpointType,endpointAddress);
					connect(topic,endpointType,endpointAddress);

				} 
				//process DISCONNECTION_REQUEST
				else if (args[0].equals(Frontend.DISCONNECTION_REQUEST)) {
					String znode = args[1];
					logger.debug("WorkerThread:{} received request:{}"
							+ "for znode:{}",workerId,Frontend.DISCONNECTION_REQUEST,znode);
					feSocket.send("received disconnection request");
				} 
				//invalid REQUEST
				else {
					logger.debug("WorkerThread:{} received invalid request:{}",
							workerId,args[0]);
					feSocket.send(String.format("Error:invalid request %s",args[0]),0);
				}
			}catch(ZMQException e){
				logger.error("WorkerThread:{} caught ZMQException",workerId);
				break;
			}
		
		}
		//Clean up: Close sockets and destroy context
		feSocket.close();
		lbSocket.close();
		context.destroy();
		logger.debug("WorkerThread:{} exited",workerId);
	}
	
	private void connect(String topic,String endpointType,String endpointAddress)
	{
		try{
			//check if topic node exists under /topics
			Stat res= client.checkExists().forPath(String.format("/topics/%s",topic));
			if(res==null){
				//new topic discovered. Block until LB allocates topic
				logger.debug("WorkerThread:{} discovered new topic:{}",
						workerId,topic);
				requestTopicCreation(topic);
			}
			//query ZK to get hosting EB locations
			List<String> ebs= queryZk(topic);
			//Select EB
			String selectedEb= selectEB(ebs);
			//get selected Eb's znode data:<eb address,topic listener port,topic sender port>
			String ebData = new String(client.getData().
				forPath(String.format("/topics/%s/%s", topic,selectedEb)));

			//create znode for connecting endpoint
			String znodePath = client.create().
					creatingParentsIfNeeded().
					withMode(CreateMode.PERSISTENT_SEQUENTIAL).
					forPath(String.format("/eb/%s/%s/%s/p",
							selectedEb,topic,endpointType),
							endpointAddress.getBytes());
			
			//reply back with selected EB and created znode path
			feSocket.send(String.format("%s;%s",ebData,znodePath),0);

		}catch(Exception e){
			logger.error("WorkerThread:{} caught exception:{}",
					workerId,e.getMessage());
			feSocket.send(String.format("Error:%s",e.getMessage()),0);
		}
	}


	private List<String> queryZk(String topic) throws Exception{
		return client.getChildren().forPath(String.format("/topics/%s", topic));
	}

	/*
	 * TODO: Currently, we are selecting any random broker 
	 * for the new end-point to connect to. 
	 * This can be changed, so that we connect
	 * to the least loaded broker.
	 */
	private String selectEB(List<String> ebs){
		int selectedBroker = (int) (Math.random() * ebs.size());
		return ebs.get(selectedBroker);
	}
	
	private void requestTopicCreation(String topic) throws Exception{
		//send request to LB
		logger.debug("WorkerThread:{} will send a topic creation request to LB for topic:{}",
				workerId,topic);
		lbSocket.send(topic,0);

		CountDownLatch latch= new CountDownLatch(1);
		//install listener callback
		NodeCache nc=new NodeCache(client,
				String.format("/topics/%s", topic));
		nc.getListenable().addListener(new NodeCacheListener(){
			@Override
			public void nodeChanged() throws Exception {
				logger.debug("WorkerThread:{} NodeCacheListener called back.",
						workerId);
				latch.countDown();
			}
		});
		nc.start();

		//block until notified
		logger.debug("WorkerThread:{} will wait until topic:{} is created",
				workerId,topic);
		latch.await();
		nc.close();
	}
	

}
