package edu.vanderbilt.edgent.fe;

import java.util.HashMap;
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
import org.apache.zookeeper.KeeperException;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import edu.vanderbilt.edgent.endpoints.Container;
import edu.vanderbilt.edgent.loadbalancer.LoadBalancer;
import edu.vanderbilt.edgent.types.FeRequest;
import edu.vanderbilt.edgent.types.FeRequestHelper;
import edu.vanderbilt.edgent.types.FeResponseHelper;
import edu.vanderbilt.edgent.util.Commands;
import edu.vanderbilt.edgent.util.PortList;

public class FeWorkerThread implements Runnable {
	/* Time period for which FE will wait for topic creation 
	 * after which if topic creation failed/timed-out, then
	 * an error message is sent back to the client.
	 */
	private static final int TOPIC_CREATION_TIMEOUT_SEC=10;

	//Shadowed ZContext 
	private ZContext context;
	//ZMQ REP Socket to process connection requests 
	private ZMQ.Socket feSocket;  
	//ZMQ PUSH socket to send topic creation requests to LB 
	private ZMQ.Socket lbSocket; 
	
	private FeResponseHelper feResponseHelper;

	//load balancer address
	private String lbAddress;
	
	//Curator client to talk to ZK
	private CuratorFramework client;

	//Worker Thread Id
	private String workerId;
	private Logger logger;

	public FeWorkerThread(ZContext context,
			CuratorFramework client,String lbAddress){
		logger=LogManager.getLogger(this.getClass().getName());
		this.context=context;
		this.client=client;
		this.lbAddress=lbAddress;
		feResponseHelper=new FeResponseHelper();
	}

	@Override
	public void run() {
		workerId=Thread.currentThread().getName();
		logger.debug("FeWorkerThread:{} started",workerId);

		//connect to FE to receive incoming requests
		feSocket= context.createSocket(ZMQ.REP);
		feSocket.connect(Frontend.INPROC_CONNECTOR);
	
		//connect to LB to send topic creation requests
		lbSocket=context.createSocket(ZMQ.PUSH);
		lbSocket.connect(String.format("tcp://%s:%d",
				lbAddress,PortList.LB_LISTENER_PORT));

		//Listening loop
		while(!Thread.currentThread().isInterrupted()){
			try{
				FeRequest request= FeRequestHelper.deserailize(feSocket.recv());
				int type=request.type();
				//process Connection request
				if(type==Commands.FE_CONNECT_REQUEST){
					String topicName=request.topicName();
					String endpointType=request.endpointType();
					String containerId=request.containerId();
					int interval=request.interval();
					logger.info("WorkerThread:{} received FE_CONNECT_REQUEST "
							+ "for topic:{}, endpointType:{} and containerId:{} interval:{}",
							workerId,topicName,endpointType,containerId,interval);
					connect(topicName,endpointType,containerId,interval);
				}
				//process Connect to EB request
				else if(type==Commands.FE_CONNECT_TO_EB_REQUEST){
					String topicName=request.topicName();
					String endpointType=request.endpointType();
					String containerId=request.containerId();
					String ebId=request.ebId();
					String experimentType=request.experimentType();
					logger.info("WorkerThread:{} received  FE_CONNECT_TO_EB_REQUEST "
							+ "for topic:{}, endpointType:{}, containerId:{}, ebId:{} and experimentType:{}",
							workerId, topicName, endpointType, containerId, ebId, experimentType);
					connectToEb(topicName,endpointType,containerId,ebId,experimentType);
				}
				//process Disconnection request
				else if(type==Commands.FE_DISCONNECT_REQUEST){
					String topicName=request.topicName();
					String endpointType=request.endpointType();
					String containerId=request.containerId();
					String ebId=request.ebId();
					String experimentType=request.experimentType();
					logger.info("WorkerThread:{} received FE_DISCONNECT_REQUEST"
							+ "for topic:{}, endpointType:{}, containerId:{},ebId:{} and experimentType:{}",
							workerId,topicName,endpointType,containerId,ebId,experimentType);
					disconnect(topicName,endpointType,containerId,ebId,experimentType);
				}
				//invalid request
				else{
					logger.error("WorkerThread:{} received invalid request type:{}",
							workerId,request.type());
					feSocket.send(feResponseHelper.serialize(Frontend.FE_RESPONSE_CODE_ERROR,
							String.format("Invalid Request:%d",type)));
				}

			}catch(ZMQException e){
				logger.error("WorkerThread:{} caught ZMQException:{}",workerId,e.getMessage());
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

	//process connection request
	private void connect(String topic,String endpointType,String containerId,int interval)
	{
		try{
			//query ZK to get hosting EB locations
			logger.info("WorkerThread:{} will query ZK to find ebs hosting topic:{}",
				workerId,topic);
			List<String> ebs= hostingEbs(topic);

			//there are no EBs hosting topic of interest. Request topic creation
			if(ebs==null || ebs.isEmpty()){
				logger.info("WorkerThread:{} no ebs host topic:{}. Will request LB to create topic:{}",
						workerId,topic,topic);
				requestTopicCreation(topic,interval);
				ebs=hostingEbs(topic);
			}

			if(ebs==null || ebs.isEmpty()){//topic creation failed
				logger.error("WorkerThread:{} failed to locate EB for topic:{}",workerId,topic);
				feSocket.send(feResponseHelper.serialize(Frontend.FE_RESPONSE_CODE_ERROR,
						String.format("Failed to locate topic:%s",topic)));
				return;
			}
			//topic creation was successful or topic already exists
			else{
				
				//query about replication mode for the topic
				String replicationMode= replicationMode(topic);
				//construct feResponse
				HashMap<String,String> feResponse= feResponse(ebs,topic,endpointType,
						containerId,replicationMode);
				//send response
				if(feResponse!=null){
					feSocket.send(feResponseHelper.serialize(Frontend.FE_RESPONSE_CODE_OK,feResponse));
				}else{
					feSocket.send(feResponseHelper.serialize(Frontend.FE_RESPONSE_CODE_ERROR,
							"FeResponse creation error"));
				}
			}

		}catch(Exception e){
			logger.error("WorkerThread:{} caught exception:{}",
					workerId,e.getMessage());
			feSocket.send(feResponseHelper.serialize(Frontend.FE_RESPONSE_CODE_ERROR,e.toString()));
		}
	}

	private List<String> hostingEbs(String topicName) {
		List<String> ebs=null;
		try{
			//list of ebs currently hosting topicName
			ebs=client.getChildren().forPath(String.format("/topics/%s", topicName));
		}catch(KeeperException e){
			if(e.code()==KeeperException.Code.NONODE){
				logger.debug("WorkerThread:{} topic:{} does not exist",workerId,topicName);
			}
		}catch(Exception e){
			logger.error("WorkerThread:{} caught exception:{}",workerId,e.getMessage());
		}
		return ebs;
	}


	private void requestTopicCreation(String topicName,int interval) throws Exception{
		//send request to LB
		lbSocket.send(String.format("%s,%d", topicName,interval));
		
		//waitset to wait on until Topic creation completes
		CountDownLatch latch= new CountDownLatch(1);

		//Listener callback to listen for EB assignment to topic of interest
		PathChildrenCache cache= new PathChildrenCache(client,
				String.format("/topics/%s",topicName ),false);
		cache.getListenable().addListener(new PathChildrenCacheListener(){

			@Override
			public void childEvent(CuratorFramework client, 
					PathChildrenCacheEvent event) throws Exception {
				if(event.getType().equals(Type.CHILD_ADDED)){
					logger.debug("WorkerThread:{} eb:{} was assigned to topic:{}",
							workerId,event.getData().getPath(),topicName);
					latch.countDown();
				}
			}
		});
		cache.start();

		//block until notified
		logger.info("WorkerThread:{} will wait for topic:{} creation",
				workerId,topicName);
		latch.await(TOPIC_CREATION_TIMEOUT_SEC, TimeUnit.SECONDS);
		cache.close();
		logger.debug("WorkerThread:{} unblocked",
				workerId,topicName);
	}

	private String replicationMode(String topicName){
		String replicationMode=null;
		try{
			//get the replication mode for topicName
			String result= new String(client.getData().forPath(String.format("/topics/%s", topicName)));
			String[] parts= result.split(",");
			replicationMode=parts[1];
		}catch(Exception e){
			logger.error("WorkerThread:{} caught exception:{}",workerId,e.getMessage());
		}
		return replicationMode;
	}

	private HashMap<String,String> feResponse(List<String> ebs,String topicName,String endpointType,
			String containerId,String replicationMode){
		if(replicationMode!=null){
			if (replicationMode.equals(LoadBalancer.REPLICATION_ALL_SUB)) {
				if (endpointType.equals(Container.ENDPOINT_TYPE_SUB)) {
					/*If replication mode is REPLICATION_ALL_SUB and endpoint type is SUB, 
					 * then the endpoint must connect to all hosting EBs on which topic is 
					 * replicated.
					 */
					return allEbs(ebs,topicName,endpointType,containerId);
				}
			} else if (replicationMode.equals(LoadBalancer.REPLICATION_ALL_PUB)) {
				if (endpointType.equals(Container.ENDPOINT_TYPE_PUB)) {
					/*If replication mode is REPLICATION_ALL_PUB and endpoint type is PUB, 
					 * then the endpoint must connect to all hosting EBs on which topic is 
					 * replicated.
					 */
					return allEbs(ebs,topicName,endpointType,containerId);
				}
			}
			/*
			 * In all other cases, the endpoint can connect to any randomly chosen 
			 * EB hosting the topic of interest. 
			 */
			return singleEb(ebs,topicName,endpointType,containerId);
		}
		return null;
	}

	/*
	 * Returns a single ebId to topicConnector mapping for a randomly chosen EB.
	 */
	private HashMap<String,String> singleEb(List<String> ebs,String topicName,
			String endpointType, String containerId){
		int selectedBroker = (int) (Math.random() * ebs.size());
		String ebId= ebs.get(selectedBroker);
		try{
			HashMap<String,String> response= new HashMap<String,String>();
			//Get topic connector data at: /topics/topicName/ebId
			String ebData = new String(client.getData().forPath(String.format("/topics/%s/%s", topicName, ebId)));
			response.put(ebId, ebData);
			return response;
		}catch(Exception e){
			logger.error("WorkerThread:{} caught exception:{}",workerId,e.getMessage());
			return null;
		}
	}

	/*
	 * Returns all the ebId to topicConnector mapping for all EBs hosting the topic of interest.
	 */
	private HashMap<String,String> allEbs(List<String> ebs,String topic,String endpointType,String containerId){
		try{
			HashMap<String,String> response= new HashMap<String,String>();
			for (String ebId : ebs) {
				//Get topic connector data at: /topics/topicName/ebId
				String ebData = new String(client.getData().forPath(String.format("/topics/%s/%s", topic, ebId)));
				response.put(ebId,ebData);
			}
			return response;
		}catch(Exception e){
			logger.error("WorkerThread:{} caught exception:{}",workerId,e.getMessage());
			return null;
		}
	}

	//Processes connect to EB request 
	private void connectToEb(String topicName,String endpointType,
			String containerId,String ebId,String experimentType){
		try{
			//NOTE: only for coordinated experiment execution 
			client.create().forPath(String.format("/experiment/%s/%s/%s-%s",experimentType,endpointType,containerId,ebId));
			//create this connecting endpoint's znode under: /eb/ebId/topicName/endpointType/containerId
			client.create()
					.forPath(String.format("/eb/%s/%s/%s/%s", ebId, topicName, endpointType,containerId));
			feSocket.send(feResponseHelper.serialize(Frontend.FE_RESPONSE_CODE_OK));
		}catch(Exception e){
			logger.error("WorkerThread:{} caught exception:{}",workerId,e.getMessage());
			feSocket.send(feResponseHelper.serialize(Frontend.FE_RESPONSE_CODE_ERROR,e.toString()));
		}
	}

	//processes disconnection request 
	private void disconnect(String topicName,String endpointType,String containerId,String ebId,String experimentType){
		String znodePath=String.format("/eb/%s/%s/%s/%s",ebId,topicName,endpointType,containerId);
		//delete the znode of the endpoint leaving the system at: /eb/ebId/topicName/enpointType/containerId
		deleteZnode(znodePath,experimentType);
	}
	
	private void deleteZnode(String znode,String experimentType){
		// NOTE: This is only for the purpose of coordinating experiment runs
		String[] parts = znode.split("/");
		String ebId = parts[2];
		String endpointType = parts[4];
		String containerId = parts[5];
		String experiment_path = String.format("/experiment/%s/%s/%s-%s", experimentType, endpointType, containerId,
				ebId);
		try {
			client.delete().forPath(experiment_path);
		} catch (KeeperException e) {
			if (e.code() == KeeperException.Code.NONODE) {
				logger.error("WorkerThread:{} znode:{} does not exist", workerId, experiment_path);
			}
		} catch (Exception e) {
			logger.error("WorkerThread:{} caught exception:{}", workerId, e.getMessage());
		}

		try {
			client.delete().forPath(znode);
			logger.debug("WorkerThread:{} deleted client znode:{}", workerId, znode);
			feSocket.send(feResponseHelper.serialize(Frontend.FE_RESPONSE_CODE_OK));
		} catch (KeeperException e) {
			if (e.code() == KeeperException.Code.NONODE) {
				logger.error("WorkerThread:{} znode:{} does not exist", workerId, znode);
				feSocket.send(feResponseHelper.serialize(Frontend.FE_RESPONSE_CODE_ERROR,
						String.format("Znode:%s does not exist", znode)));
			}
		} catch (Exception e) {
			logger.error("WorkerThread:{} caught exception:{}", workerId, e.getMessage());
			feSocket.send(feResponseHelper.serialize(Frontend.FE_RESPONSE_CODE_ERROR, e.getMessage()));
		}
				
	}

}
