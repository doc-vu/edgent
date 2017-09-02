package edu.vanderbilt.edgent.brokers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import edu.vanderbilt.edgent.util.PortList;
import edu.vanderbilt.edgent.util.UtilMethods;

/**
 * EdgeBroker managing local/intra-region data dissemination concerns.
 * @author kharesp
 */
public class EdgeBroker implements Runnable{
	//Port at which broker issues topic control messages for hosted topics
	public static final int TOPIC_CONTROL_PORT=4993;
	//Topic control messages
	public static final String TOPIC_DELETE_COMMAND="DELETE_TOPIC";
	public static final String TOPIC_CREATION_COMMAND="CREATE_TOPIC";
	public static final String TOPIC_LB_COMMAND="LB_TOPIC";
	public static final String EB_EXIT_COMMAND="EXIT_EB";

	//Periodic interval after which pruning thread is scheduled
	public static final int PERIODIC_PRUNING_INTERVAL_SEC=120;

	//List of avaiable ports
	private PortList ports;
	//ZMQ Context
	private ZMQ.Context context;
	//ZMQ PUB socket to issue topic control messages for hosted topics
	private ZMQ.Socket topicControl;

	//Curator client for ZK connection
	private CuratorFramework client;

	//Maintains references to currently hosted topics
	private HashMap<String,Topic> hostedTopics;
	//Maintains references to currently hosted topic threads
	private HashMap<String,Thread> topicThreads;
	/*Maintains references PathChildrenCache listeners for topic znode /lb/topics/topic to 
	listen for topic level rebalancing commands issued by LB*/
	private HashMap<String,PathChildrenCache> topicLbListeners;

	//ZK path children listener, to listen for topic assignment to this broker
	private PathChildrenCache topicAssignmentListener;

	//Executor to run the topic clean-up task periodically
	private ScheduledExecutorService scheduler;

	//Shared queue in which ZK callbacks place commands for EB to act on
	private LinkedBlockingQueue<String> queue;
	
	//This broker's ipAddress,regionId and id
	private String ipAddress;
	private int regionId;
	private String ebId;
	
	private Logger logger;

	public EdgeBroker(String zkConnector,int ioThreads){
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		ipAddress= UtilMethods.ipAddress();
		regionId= UtilMethods.regionId(); 
		ebId=String.format("EB-%d-%s",regionId,ipAddress);

		this.context = ZMQ.context(ioThreads);
		//initialize state information
		hostedTopics= new HashMap<String,Topic>();
		topicThreads= new HashMap<String,Thread>();
		topicLbListeners=new HashMap<String,PathChildrenCache>();
		
		//singleton list of available port numbers
		ports= PortList.getInstance();

		//queue of commands for this EB
		queue= new LinkedBlockingQueue<String>();

		//initialize curator client for ZK connection
		client=CuratorFrameworkFactory.newClient(zkConnector,
				new ExponentialBackoffRetry(1000, 3));
		client.start();
		
		logger.info("Initialized EdgeBroker:{}",ebId);
	}


	/**
	 * Entry-point for EB thread
	 */
	@Override
	public void run()
	{
		try{
			logger.info("EdgeBroker:{} started",ebId);

			//Initialize ZMQ topic control socket
			topicControl = context.socket(ZMQ.PUB);
			topicControl.bind(String.format("tcp://*:%d", TOPIC_CONTROL_PORT));

			//create eb znode under /eb 
			client.create().forPath(String.format("/eb/%s",ebId));
			logger.info("EB:{} created its znode under /eb",ebId);

			//register child listener for this broker's znode
			topicAssignmentListener= new PathChildrenCache(client,
					String.format("/eb/%s",ebId),true);
			topicAssignmentListener.getListenable().addListener(new PathChildrenCacheListener(){
				@Override
				public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
					//new topic was assigned to this EB 
					if(event.getType()==Type.CHILD_ADDED){
						String path=event.getData().getPath();
						String topicName=path.split("/")[3];
						logger.info("EB:{} was assigned topic:{}",ebId,topicName);
						//place TOPIC_CREATION_COMMAND in EB's queue
						queue.add(String.format("%s,%s",TOPIC_CREATION_COMMAND,topicName));
					}
				}
			});
			//start listening for topic assignment to this EB
			topicAssignmentListener.start();

			// schedule periodic pruning of topics hosted on this EB
			scheduler = Executors.newScheduledThreadPool(1);
			scheduler.scheduleWithFixedDelay(new Prune(ebId,client,queue), 
					PERIODIC_PRUNING_INTERVAL_SEC, PERIODIC_PRUNING_INTERVAL_SEC,
					TimeUnit.SECONDS);

			//start listening for incoming processing requests
			logger.info("EdgeBroker:{} will start listening for incoming requests",ebId);
			listen();
		
			logger.info("EdgeBroker:{} will cleanup before exiting",ebId);
			//cleanup before exiting
			cleanup();

		}catch(Exception e){
			logger.error("EB:{} caught exception:{}",
					ebId,e.getMessage());
			cleanup();
		}
		logger.info("EdgeBroker:{} exited",ebId);
	}

	/** 
	 * EB listener loop will process incoming commands 
	 */
	private void listen(){
		while(!Thread.currentThread().isInterrupted()){
			try {
				String command = queue.take();
				String[] args = command.split(",");
				String commandType=args[0];
				if(commandType.equals(TOPIC_CREATION_COMMAND)){
					String topicName=args[1];
					logger.info("EdgeBroker:{} will process command:{} for topic:{}",
							ebId,TOPIC_CREATION_COMMAND,topicName);
					createTopic(topicName);
				
				}else if(commandType.equals(TOPIC_DELETE_COMMAND)){
					String topicName=args[1];
					logger.info("EdgeBroker:{} will process command:{} for topic:{}",
							ebId,TOPIC_DELETE_COMMAND,topicName);
					deleteTopic(topicName);
					break;
				}else if(commandType.equals(TOPIC_LB_COMMAND)){
					String topicName=args[1];
					logger.info("EdgeBroker:{} will process command:{} for topic:{}",
							ebId,TOPIC_LB_COMMAND,topicName);
					testLb(topicName);
				}else if(commandType.equals(EB_EXIT_COMMAND)){
					logger.info("EdgeBroker:{} received command:{}. Will exit listener loop.",
							ebId,EB_EXIT_COMMAND);
					break;
				}else{
					logger.debug("EdgeBroker:{} received invalid command:{}",
							ebId,commandType);
				}
			} catch (InterruptedException e) {
				logger.error("EdgeBroker:{} caught exception:{}",ebId,e.getMessage());
				break;
			}
		}
	}

	/**
	 * Adds EB_EXIT_COMMAND to EB's queue to shutdown EB's listener loop
	 */
	public void shutdown(){
		queue.add(EB_EXIT_COMMAND);
	}
	
	/**
	 * Creates a topic which will be hosted on this EB. 
	 * First starts the topic thread and then creates this EB's znode 
	 * under /topics/topicName/eb to signify that the newly created topic
	 * is hosted on this EB.
	 * @param topicName 
	 */
	private void createTopic(String topicName){
		logger.debug("EdgeBroker:{} will create topic:{}",ebId,topicName);

		if(!hostedTopics.containsKey(topicName)){
			//Acquire port numbers for new topic's receive, send and control ports
			int receivePort = ports.acquire();
			int sendPort = ports.acquire();
			int lbPort = ports.acquire();

			//Instantiate topic
			Topic topic= new Topic(topicName,context,
					receivePort,sendPort,lbPort);
			//Add created topic instance to hostedTopics map
			hostedTopics.put(topicName,topic);
			
			//Start the topic thread
			Thread topicThread=new Thread(topic);
			topicThreads.put(topicName,topicThread);
			topicThread.start();
			logger.debug("EdgeBroker:{} topic:{} thread started",ebId,topicName);


			try {
				// register listener for receiving topic level LB directives for
				PathChildrenCache cache = new PathChildrenCache(client, String.format("/lb/topics/%s", topicName),
						true);
				topicLbListeners.put(topicName, cache);
				cache.getListenable().addListener(new PathChildrenCacheListener() {
					@Override
					public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
						if(event.getType()==Type.CHILD_ADDED){
							queue.add(String.format("%s,%s",TOPIC_LB_COMMAND,topicName));
						}
					}
				});
				cache.start();
				logger.debug("EdgeBroker:{} installed listener for /lb/topics/{} to receive rebalancing commands for topic:{}",
						ebId,topicName,topicName);

				// create /topics/topicName/ebId
				client.create().creatingParentsIfNeeded()
					.forPath(String.format("/topics/%s/%s", topicName, ebId),
						String.format("%s,%d,%d,%d", ipAddress, receivePort, sendPort, lbPort).getBytes());
				logger.debug("EdgeBroker:{} created its znode under /topics/{}", ebId, topicName);
			} catch (Exception e) {
				logger.error("EdgeBroker:{} caught exception:{}", ebId, e.getMessage());
			}

			logger.info("EdgeBroker:{} created topic:{}", ebId, topicName);
		}else{
			logger.error("EdgeBroker:{} topic:{} already exists",ebId,topicName);
		}
	}

	/**
	 * Deletes a topic hosted on this EB.
	 * @param topicName
	 */
	private void deleteTopic(String topicName) {
		if(hostedTopics.containsKey(topicName))
		{
			//remove Topic from hostedTopics 
			Topic topic= hostedTopics.remove(topicName);

			//remove topic thread from topicThreads map
			Thread topicThread= topicThreads.remove(topicName);

			//remove and close this topic's lb command listener
			PathChildrenCache topicLbCache= topicLbListeners.remove(topicName);
			try {
				topicLbCache.close();
			} catch (IOException e) {
				logger.error("EdgeBroker:{} caught exception:{}",ebId,e.getMessage());
			}

			logger.debug("EdgeBroker:{} will delete topic:{}",ebId,topicName);
			deleteTopic(topicName, topic,topicThread);
			logger.info("EdgeBroker:{} deleted topic:{}", ebId,topicName);
		}else{
			logger.error("EdgeBroker:{} topic:{} does not exist",ebId,topicName);
		}
	}

	/**
	 * Deletes all topics hosted on this EB
	 */
	private void deleteAllTopics()
	{
		/* HashMap's iterator is fail-fast. We can't iterate and modify the 
		 * contents at the same time. Only iterator's remove method can be used
		 * to modify contents while iterating over the contents.
		*/
		Iterator<Entry<String, Topic>> iter=hostedTopics.entrySet().iterator();
		while(iter.hasNext()){
			Entry<String, Topic> pair= iter.next();
			String topicName= pair.getKey();
			Topic topic= pair.getValue();
			//remove topic from hostedTopics map 
			iter.remove();

			//remove topic thread from topicThreads map
			Thread topicThread= topicThreads.remove(pair.getKey());

			//remove and close this topic's lb command listener
			PathChildrenCache topicLbCache= topicLbListeners.remove(topicName);
			try {
				topicLbCache.close();
			} catch (IOException e) {
				logger.error("EdgeBroker:{} caught exception:{}",ebId,e.getMessage());
			}

			logger.debug("EdgeBroker:{} will delete topic:{}",ebId,topicName);
			deleteTopic(topicName,topic,topicThread);
			logger.info("EdgeBroker:{} deleted topic:{}",ebId,topicName);
		}
	}

	/**
	 * Helper method for deleting hosted topic. Method first removes this EB's
	 * znode under /topics/topicName/ebId to indicate that this topic is 
	 * no longer hosted on this EB. 
	 * If there are no other EBs in the system also hosting this topic,
	 * then it removes the topic's znode at /topics/topicName and /lb/topics/topicName,
	 * to indicate that this topic no longer exists in the system. 
	 * It deletes this topic's sub-tree under /eb/ebId to signify that topic is not 
	 * hosted on this EB.
	 * @param topicName
	 * @param topic
	 * @param tThread
	 */
	private void deleteTopic(String topicName, Topic topic, Thread tThread) {
		try {
			// delete this EB's znode under /topics/topic
			client.delete().forPath(String.format("/topics/%s/%s", topicName, ebId));
			logger.debug("EdgeBroker:{} deleted its znode /topics/{}/{}",ebId,topicName,ebId);


			//check if there are other brokers hosting the topic, if not, delete topic znode
			List<String> hosting_ebs= client.getChildren().forPath(String.format("/topics/%s", topicName));
			if(hosting_ebs.isEmpty()){
				client.delete().forPath(String.format("/topics/%s", topicName));
				client.delete().deletingChildrenIfNeeded().forPath(String.format("/lb/topics/%s", topicName));
				logger.debug("EdgeBroker:{} deleted topic:{} from the system as there are no hosting EBs",ebId,topicName);
			}

			// delete /eb/ebId/topic/*
			client.delete().deletingChildrenIfNeeded().forPath(String.format("/eb/%s/%s", ebId, topicName));
			logger.debug("EdgeBroker:{} deleted topic:{}'s sub-tree under its znode", ebId, topicName);

			// send control message to topic thread to exit polling loop
			logger.debug("EdgeBroker:{} will send:{} control message to topic:{} thread",
					ebId,TOPIC_DELETE_COMMAND,topicName);
			topicControl.send(String.format("%s %s", topicName, TOPIC_DELETE_COMMAND));

			// wait until topic thread exits
			tThread.join();
			logger.debug("EdgeBroker:{} topic:{} thread has exited",
					ebId,topicName);

			// release receiver and sender port numbers of this topic for reuse
			ports.release(topic.receivePort());
			ports.release(topic.sendPort());
			ports.release(topic.lbPort());
		} catch (Exception e) {
			logger.error("EdgeBroker:{} caught exception:{}", ebId, e.getMessage());
		}
	}

	private void testLb(String topic){
		for(int i=0;i<10;i++){
			topicControl.send(String.format("%s %s",topic,EdgeBroker.TOPIC_LB_COMMAND));
		}
	}

	/**
	 * Performs clean up before exiting
	 */
	private void cleanup() 
	{
		//shutdown scheduled executor service 
		scheduler.shutdown();
		logger.debug("EdgeBroker:{} shutdown periodic clean-up scheduler",ebId);

		//delete all hosted topics at the broker
		logger.debug("EdgeBroker:{} will delete all hosted topics",ebId);
		deleteAllTopics();

		//close ZMQ topic control socket
		topicControl.setLinger(0);
		topicControl.close();
		//close ZMQ Context
		context.term();
		logger.debug("EdgeBroker:{} closed ZMQ sockets and context",ebId);
		
		try {
			if(topicAssignmentListener!=null){
				topicAssignmentListener.close();
			}
			logger.debug("EdgeBroker:{} closed its topic assignment listner",ebId);

			client.delete().forPath(String.format("/eb/%s",ebId));
			logger.debug("EdgeBroker:{} deleted its znode under /eb",ebId);
		} catch (Exception e) {
			logger.error("EdgeBroker:{} caught exception:{}",
					ebId,e.getMessage());
		}
		
		CloseableUtils.closeQuietly(client);
		logger.debug("EdgeBroker:{} closed ZK connection",ebId);
	}
	
	
	public static void main(String args[]){
		if(args.length<2){
			System.out.println("Usage: EdgeBroker zkConnector ioThreads");
			return;
		}
		try{
			//parse commandline arguments
			String zkConnector=args[0];
			int ioThreads=Integer.parseInt(args[1]);

			//initialize EB 
			EdgeBroker eb= new EdgeBroker(zkConnector,
					ioThreads);
			Thread ebThread= new Thread(eb);

			//callback to handle SIGINT and SIGTERM
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					//shutdown EB's execution
					eb.shutdown();
					try{
						//wait until EB exits
						ebThread.join();
					}catch(InterruptedException e){}
				}
			});

			//start Eb
			ebThread.start();

		}catch(NumberFormatException e){
			e.printStackTrace();
		} 
	}

}
