package edu.vanderbilt.edgent.fe;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import edu.vanderbilt.edgent.loadbalancing.LoadBalancer;

public class FeWorkerThread implements Runnable {
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
	
	private static final int TOPIC_CREATION_TIMEOUT_SEC=10;

	public FeWorkerThread(ZContext context,
			CuratorFramework client,String lbAddress){
		logger=LogManager.getLogger(this.getClass().getSimpleName());
		this.context=context;
		this.client=client;
		this.lbAddress=lbAddress;
	}

	@Override
	public void run() {
		workerId=Thread.currentThread().getName();
		logger.info("WorkerThread:{} started",workerId);

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
					if(args.length==5){
						String znodePath=args[4];
						logger.info("WorkerThread:{} received a re-connection request "
							+ "for topic:{}, endpointType:{},ip:{}."
							+ "Will delete pre-existing client znode:{}",workerId,topic,
							endpointType,endpointAddress,znodePath);
						deleteZnode(znodePath);
					}else{
						logger.info("WorkerThread:{} received request:{} "
								+ "for topic:{}, endpointType:{} and ip:{} ",workerId,
								Frontend.CONNECTION_REQUEST,topic,endpointType,endpointAddress);
					}
					connect(topic,endpointType,endpointAddress);
				} 
				//process DISCONNECTION_REQUEST
				else if (args[0].equals(Frontend.DISCONNECTION_REQUEST)) {
					String znode = args[1];
					logger.info("WorkerThread:{} received request:{} "
							+ "for znode:{}",workerId,Frontend.DISCONNECTION_REQUEST,znode);
					deleteZnode(znode);
					feSocket.send("ok");
				} 
				//invalid REQUEST
				else {
					logger.error("WorkerThread:{} received invalid request:{}",
							workerId,args[0]);
					feSocket.send(String.format("Error:invalid request %s",args[0]));
				}
			}catch(ZMQException e){
				logger.error("WorkerThread:{} caught ZMQException",workerId);
				break;
			}
		
		}
		//Set linger to 0
		feSocket.setLinger(0);
		lbSocket.setLinger(0);
		//Close sockets 
		feSocket.close();
		lbSocket.close();
		//destroy context
		context.destroy();
		logger.debug("WorkerThread:{} closed ZMQ sockets and context",workerId);
		logger.info("WorkerThread:{} exited cleanly",workerId);
	}

	private void deleteZnode(String znode){
		try {
			client.delete().forPath(znode);
			logger.debug("WorkerThread:{} deleted client znode:{}",workerId,
					znode);
		}catch(KeeperException e){
			if(e.code()==KeeperException.Code.NONODE){
				logger.info("WorkerThread:{} znode:{} does not exist",workerId,znode);
			}
		}catch (Exception e) {
			logger.error("WorkerThread:{} caught exception:{}",
					workerId,e.getMessage());
		}
	}
	
	private void connect(String topic,String endpointType,String endpointAddress)
	{
		try{
			//query ZK to get hosting EB locations
			logger.info("WorkerThread:{} will query ZK to find ebs hosting topic:{}",
				workerId,topic);
			List<String> ebs= queryZk(topic);

			//there are no EBs hosting topic of interest. Request topic creation
			if(ebs==null || ebs.isEmpty()){
				logger.info("WorkerThread:{} no ebs host topic:{}. Will request LB to create topic:{}",
						workerId,topic,topic);
				requestTopicCreation(topic);
			}

			ebs=queryZk(topic);
			if(ebs==null || ebs.isEmpty()){//topic creation failed
				logger.error("WorkerThread:{} failed to locate EB for topic:{}",workerId,topic);
				feSocket.send("Error:Failed to locate topic");
				return;
			}else{//topic creation was successful
				String selectedEb = selectEB(ebs);
				// get selected Eb's znode data:<eb address,topic listener
				// port,topic sender port>
				String ebData = new String(client.getData().forPath(String.format("/topics/%s/%s", topic, selectedEb)));

				logger.info("WorkerThread:{} selected eb:{} to connect to, for topic:{}", workerId, ebData, topic);

				// create znode for connecting endpoint
				String znodePath = client.create().
						withMode(CreateMode.PERSISTENT_SEQUENTIAL).
						forPath(String.format("/eb/%s/%s/%s/c", selectedEb, topic, endpointType),
								endpointAddress.getBytes());
				logger.info("WorkerThread:{} created client znode:{}", workerId, znodePath);

				// reply back with selected EB and created znode path
				feSocket.send(String.format("%s;%s", ebData, znodePath), 0);
			}

		}catch(Exception e){
			logger.error("WorkerThread:{} caught exception:{}",
					workerId,e.getMessage());
			feSocket.send(String.format("Error:%s",e.getMessage()),0);
		}
	}

	private List<String> queryZk(String topic) {
		List<String> ebs=null;
		try{
			ebs=client.getChildren().forPath(String.format("/topics/%s", topic));
		}catch(KeeperException e){
			if(e.code()==KeeperException.Code.NONODE){
				logger.debug("WorkerThread:{} topic:{} does not exist",workerId,topic);
			}
		}catch(Exception e){
			logger.error("WorkerThread:{} caught exception:{}",workerId,e.getMessage());
		}
		return ebs;
	}

	private void requestTopicCreation(String topic) throws Exception{
		//send request to LB
		lbSocket.send(topic,0);
		
		//waitset to wait on until Topic creation completes
		CountDownLatch latch= new CountDownLatch(1);

		//Listener callback to listen for EB assignment to topic of interest
		PathChildrenCache cache= new PathChildrenCache(client,
				String.format("/topics/%s",topic ),false);
		cache.getListenable().addListener(new PathChildrenCacheListener(){

			@Override
			public void childEvent(CuratorFramework client, 
					PathChildrenCacheEvent event) throws Exception {
				if(event.getType().equals(Type.CHILD_ADDED)){
					logger.debug("WorkerThread:{} eb:{} was assigned to topic:{}",
							workerId,event.getData().getPath(),topic);
					latch.countDown();
				}
			}
		});
		cache.start();

		//block until notified
		logger.info("WorkerThread:{} will wait for topic:{} creation",
				workerId,topic);
		latch.await(TOPIC_CREATION_TIMEOUT_SEC, TimeUnit.SECONDS);
		cache.close();
		logger.debug("WorkerThread:{} unblocked",
				workerId,topic);
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
	

}
