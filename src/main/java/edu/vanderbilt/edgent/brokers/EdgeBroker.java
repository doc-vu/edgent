package edu.vanderbilt.edgent.brokers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
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
import edu.vanderbilt.edgent.rebalancing.AllPublisherReplication;
import edu.vanderbilt.edgent.rebalancing.AllSubscriberReplication;
import edu.vanderbilt.edgent.rebalancing.Rebalance;
import edu.vanderbilt.edgent.rebalancing.TopicMigration;
import edu.vanderbilt.edgent.types.TopicCommandHelper;
import edu.vanderbilt.edgent.util.Commands;
import edu.vanderbilt.edgent.util.PortList;
import edu.vanderbilt.edgent.util.UtilMethods;

/**
 * EdgeBroker managing local/intra-region data dissemination concerns.
 * @author kharesp
 */
public class EdgeBroker implements Runnable{
	//Periodic interval after which pruning thread is scheduled
	public static final int PERIODIC_PRUNING_INTERVAL_SEC=86400;

	//List of avaiable ports
	private PortList ports;
	//ZMQ Context
	private ZMQ.Context context;
	//ZMQ PUB socket to issue topic control messages for hosted topics
	private ZMQ.Socket topicControl;
	//connector for topicControl ZMQ.PUB socket
	private String topicControlConnector;

	//Curator client for ZK connection
	private CuratorFramework client;

	//ZK path children listener, to listen for topic assignment to this broker
	private PathChildrenCache topicAssignmentListener;

	//Maintains references to currently hosted topics
	private HashMap<String,Topic> hostedTopics;
	//Maintains references to currently hosted topic threads
	private HashMap<String,Thread> topicThreads;
	/*Maintains references PathChildrenCache listeners for topic znode /lb/topics/topic to 
	listen for topic level rebalancing commands issued by LB*/
	private HashMap<String,PathChildrenCache> topicLbListeners;

	//Executor to run the topic clean-up task periodically
	private ScheduledExecutorService scheduler;

	/* Shared queue in which ZK callbacks place commands for EB to act on.
	 * Here, using a ZMQ Pull socket to implememt the queue does not make
	 * sense, as several ZK callbacks will happen in different threads and
	 * using a ZMQ PUSH socket from different thread contexts is prohibited. 
	 * Creating a ZMQ socket each time a ZK callback happens is not feasible. 
	 */
	private LinkedBlockingQueue<String> queue;
	
	private TopicCommandHelper topicCommandHelper;
	
	//This broker's ipAddress,regionId and id
	private String ipAddress;
	private int regionId;
	private String ebId;
	private Logger logger;

	public EdgeBroker(String zkConnector,int ioThreads,int id){

		logger= LogManager.getLogger(this.getClass().getSimpleName());
		ipAddress= UtilMethods.ipAddress();
		regionId= UtilMethods.regionId(); 
		ebId=String.format("EB-%d-%s-%d",regionId,ipAddress,id);

		//topicControlConnector=String.format("inproc://%s", ebId);
		topicControlConnector=String.format("tcp://localhost:%d",PortList.EB_TOPIC_CONTROL_PORT);

		this.context = ZMQ.context(ioThreads);
		//this.context.setMaxSockets(50000);
		//initialize state information
		hostedTopics= new HashMap<String,Topic>();
		topicThreads= new HashMap<String,Thread>();
		topicLbListeners=new HashMap<String,PathChildrenCache>();
		
		topicCommandHelper= new TopicCommandHelper();
		
		//singleton list of available port numbers
		ports= PortList.getInstance(id);

		//queue of commands for this EB
		queue= new LinkedBlockingQueue<String>();
	
		//executor service on which topic pruning thread is scheduled
		scheduler = Executors.newScheduledThreadPool(1);

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
			topicControl.bind(topicControlConnector);

			//create eb znode at: /eb/ebId
			client.create().forPath(String.format("/eb/%s",ebId));
			logger.info("EB:{} created its znode under /eb",ebId);

			//register child listener for this broker's znode to listen for topic assignment under: /eb/ebId
			topicAssignmentListener= new PathChildrenCache(client,
					String.format("/eb/%s",ebId),true);
			topicAssignmentListener.getListenable().addListener(new PathChildrenCacheListener(){
				@Override
				public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
					//new topic was assigned to this EB 
					if(event.getType()==Type.CHILD_ADDED){
						//topic path: /eb/ebId/topicName
						String path=event.getData().getPath();
						String topicName=path.split("/")[3];
						String data= new String(client.getData().forPath(String.format("/topics/%s",topicName)));
						String[] parts= data.split(",");
						logger.info("EB:{} was assigned topic:{} with processing interval:{} and replication mode:{}",
								ebId,topicName,parts[0],parts[1]);
						//place EB_TOPIC_CREATE COMMAND in EB's queue
						queue.add(String.format("%s,%s,%s",Commands.EB_TOPIC_CREATE_COMMAND,topicName,parts[0]));
					}
					if(event.getType()==Type.CHILD_REMOVED){
						//topic path: /eb/ebId/topicName
						String path=event.getData().getPath();
						String topicName=path.split("/")[3];
						logger.info("EB:{} topic:{} was removed", ebId,topicName);
						queue.add(String.format("%s,%s",Commands.EB_TOPIC_DELETE_COMMAND,topicName));
					}
				}
			});
			//start listening for topic assignment to this EB
			topicAssignmentListener.start();

			// schedule periodic pruning of topics hosted on this EB
			scheduler.scheduleWithFixedDelay(new Prune(ebId,client,queue), 
					PERIODIC_PRUNING_INTERVAL_SEC, PERIODIC_PRUNING_INTERVAL_SEC,
					TimeUnit.SECONDS);

			//start listening for incoming requests
			logger.info("EdgeBroker:{} will start listening for incoming requests",ebId);
			listen();
		
			logger.info("EdgeBroker:{} will cleanup before exiting",ebId);
			//cleanup before exiting
			cleanup();

		}catch(Exception e){
			logger.error("EB:{} caught exception:{}",
					ebId,e.toString());
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
				logger.info("EdgeBroker:{} will listen",ebId);
				String command = queue.take();
				String[] args = command.split(",");
				String commandType=args[0];
				//topic creation
				if(commandType.equals(Commands.EB_TOPIC_CREATE_COMMAND)){
					String topicName=args[1];
					String interval=args[2];
					logger.info("EdgeBroker:{} will process command:{} for topic:{} and processing interval:{}",
							ebId,Commands.EB_TOPIC_CREATE_COMMAND,topicName,interval);
					createTopic(topicName,interval);
				
				}
				//topic deletion
				else if(commandType.equals(Commands.EB_TOPIC_DELETE_COMMAND)){
					String topicName=args[1];
					logger.info("EdgeBroker:{} will process command:{} for topic:{}",
							ebId,Commands.EB_TOPIC_DELETE_COMMAND,topicName);
					deleteTopic(topicName);
				}
				//topic LB
				else if(commandType.equals(Commands.EB_TOPIC_LB_COMMAND)){
					String topicName=args[1];
					String lbPolicy=args[2];
					logger.info("EdgeBroker:{} will process command:{} for topic:{} with LB policy:{}",
							ebId,Commands.EB_TOPIC_LB_COMMAND,topicName,lbPolicy);

					if(lbPolicy.equals(Rebalance.LB_POLICY_ALL_PUB)){
						try{
							client.delete().forPath(String.format("/topics/%s/%s", topicName, ebId));
							String topicConnector = hostedTopics.get(topicName).topicConnector();
							Rebalance strategy = new AllPublisherReplication(topicName, ebId, topicConnector, false,
									client, topicControl,topicCommandHelper);
							strategy.execute();
							client.create().forPath(String.format("/topics/%s/%s", topicName, ebId),topicConnector.getBytes());
						}catch(Exception e){
							logger.error("EdgeBroker:{} caught exception:{}",ebId,e.getMessage());
						}
					}
					else if(lbPolicy.equals(Rebalance.LB_POLICY_ALL_SUB)){
						try{
							client.delete().forPath(String.format("/topics/%s/%s", topicName, ebId));
							String topicConnector = hostedTopics.get(topicName).topicConnector();
							Rebalance strategy = new AllSubscriberReplication(topicName, ebId, topicConnector, false,
									client, topicControl,topicCommandHelper);
							strategy.execute();
							client.create().forPath(String.format("/topics/%s/%s", topicName, ebId),topicConnector.getBytes());
						}catch(Exception e){
							logger.error("EdgeBroker:{} caught exception:{}",ebId,e.getMessage());
						}
					}else if(lbPolicy.equals(Rebalance.LB_POLICY_MIGRATION)){
						try{
							client.delete().forPath(String.format("/topics/%s/%s", topicName, ebId));
							String topicConnector = hostedTopics.get(topicName).topicConnector();
							Rebalance strategy = new TopicMigration(topicName, ebId, topicConnector, true,
									client, topicControl,topicCommandHelper);
							strategy.execute();
							deleteTopic(topicName);
						}catch(Exception e){
							logger.error("EdgeBroker:{} caught exception:{}",ebId,e.getMessage());
						}
						
					}else{
						logger.error("EdgeBroker:{} LB Policy:{} not recognized",ebId,lbPolicy);
					}
				}
				//EB exit
				else if(commandType.equals(Commands.EB_EXIT_COMMAND)){
					logger.info("EdgeBroker:{} received command:{}. Will exit listener loop.",
							ebId,Commands.EB_EXIT_COMMAND);
					break;
				}
				//invalid command
				else{
					logger.error("EdgeBroker:{} received invalid command:{}",
							ebId,commandType);
				}
			} catch (InterruptedException e) {
				logger.error("EdgeBroker:{} caught exception:{}",ebId,e.getMessage());
				break;
			}
		}
	}

	/**
	 * Creates a topic which will be hosted on this EB. 
	 * First starts the topic thread and then creates this EB's znode 
	 * under /topics/topicName/eb to signify that the newly created topic
	 * is hosted on this EB.
	 * @param topicName 
	 */
	private void createTopic(String topicName,String interval){
		logger.debug("EdgeBroker:{} will create topic:{}",ebId,topicName);

		if(!hostedTopics.containsKey(topicName)){
			//Acquire port numbers for new topic's receive, send and control ports
			int receivePort = ports.acquire();
			int sendPort = ports.acquire();
			int lbPort = ports.acquire();
			logger.debug("EdgeBroker:{} acquired receivePort:{}, sendPort:{} and lbPort:{} for topic:{}",
					ebId,receivePort,sendPort,lbPort,topicName);

			CountDownLatch topicInitialized=new CountDownLatch(1);
			//Instantiate topic
			Topic topic= new Topic(topicName,context,topicControlConnector,
					receivePort,sendPort,lbPort,topicInitialized,Integer.parseInt(interval));
			//Start the topic thread
			Thread topicThread=new Thread(topic);
			topicThread.start();

			try{
				//wait until topic thread is properly initialized and listening for incoming data
				topicInitialized.await();
				if(!topic.listening()){
					logger.error("EdgeBroker:{} could not create topic:{}",ebId,topicName);
					deleteTopicZkPaths(topicName);
					return;
				}
			}catch(InterruptedException e){
				logger.error("EdgeBroker:{} caught exception: {}",ebId,e.getMessage());
				deleteTopicZkPaths(topicName);
				return;
			}

			//Add created topic instance to hostedTopics map
			hostedTopics.put(topicName,topic);
			topicThreads.put(topicName,topicThread);
			
			logger.debug("EdgeBroker:{} topic:{} thread started",ebId,topicName);


			try {
				//create: /eb/ebId/topicName/pub
				client.create().forPath(String.format("/eb/%s/%s/pub", ebId, topicName));
				//create: /eb/ebId/topicName/sub
				client.create().forPath(String.format("/eb/%s/%s/sub", ebId, topicName));
				//create: /topics/topicName/ebId
				client.create().forPath(String.format("/topics/%s/%s", topicName, ebId),
						String.format("%s,%d,%d,%d", ipAddress, receivePort, sendPort, lbPort).getBytes());
				logger.debug("EdgeBroker:{} created its znode under /topics/{}/{}", ebId, topicName,ebId);
				//create: /lb/topics/topicName/ebId
				client.create().forPath(String.format("/lb/topics/%s/%s", topicName, ebId));
				logger.debug("EdgeBroker:{} created its znode under /lb/topics/{}/{}", ebId, topicName,ebId);

				// register listener for receiving topic level LB directives for this EB under: /lb/topics/topicName/ebId
				@SuppressWarnings("resource")
				PathChildrenCache cache = new PathChildrenCache(client, 
						String.format("/lb/topics/%s/%s",topicName,ebId),true);
				topicLbListeners.put(topicName, cache);
				cache.getListenable().addListener(new PathChildrenCacheListener() {
					@Override
					public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
						if(event.getType()==Type.CHILD_ADDED){
							//path: /lb/topics/topicName/ebId/lbPolicy
							String znodePath= event.getData().getPath();
							String[] parts= znodePath.split("/");
							String topicName=parts[3];
							String lbPolicy=parts[5];
							//enqueue the topic level LB directive for the EB to process
							queue.add(String.format("%s,%s,%s",Commands.EB_TOPIC_LB_COMMAND,topicName,lbPolicy));
						}
					}
				});
				cache.start();
				logger.debug("EdgeBroker:{} installed listener for /lb/topics/{}/{} to receive rebalancing commands for topic:{}",
						ebId,topicName,ebId,topicName);

				logger.info("EdgeBroker:{} created topic:{}", ebId, topicName);
			} catch (Exception e) {
				logger.error("EdgeBroker:{} caught exception:{}", ebId, e.getMessage());
				deleteTopicZkPaths(topicName);
			}

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
			deleteTopicZkPaths(topicName);
			// send control message to topic thread to exit polling loop
			logger.debug("EdgeBroker:{} will send TOPIC_DELETE_COMMNAD control message to topic:{} thread",
					ebId,topicName);
			topicControl.sendMore(topicName.getBytes());
			topicControl.send(topicCommandHelper.serialize(Commands.TOPIC_DELETE_COMMAND));

			// wait until topic thread exits
			tThread.join();
			logger.debug("EdgeBroker:{} topic:{} thread has exited",
					ebId,topicName);

			// release receiver,sender and lb ports for this topic for reuse
			ports.release(topic.receivePort());
			ports.release(topic.sendPort());
			ports.release(topic.lbPort());
		} catch (Exception e) {
			logger.error("EdgeBroker:{} caught exception:{}", ebId, e.getMessage());
		}
	}
	
	/**
	 * Adds EB_EXIT_COMMAND to EB's queue to shutdown EB's listener loop
	 */
	public void shutdown(){
		queue.add(Commands.EB_EXIT_COMMAND);
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
		context.close();
		context.term();
		logger.debug("EdgeBroker:{} closed ZMQ sockets and context",ebId);
		
		try {
			//close topicAssignmentListener cache
			if(topicAssignmentListener!=null){
				topicAssignmentListener.close();
			}
			logger.debug("EdgeBroker:{} closed its topic assignment listner",ebId);

			//delete this EB's znode at: /eb/ebId
			client.delete().forPath(String.format("/eb/%s",ebId));
			logger.debug("EdgeBroker:{} deleted its znode under /eb",ebId);
		} catch (Exception e) {
			logger.error("EdgeBroker:{} caught exception:{}",
					ebId,e.getMessage());
		}
		
		CloseableUtils.closeQuietly(client);
		logger.debug("EdgeBroker:{} closed ZK connection",ebId);
		logger.info("EdgeBroker:{} exited cleanly", ebId);
	}

	private void deleteTopicZkPaths(String topicName){
		try{
			// delete this EB's znode under: /topics/topicName/ebId if it exists
			if(client.checkExists().forPath(String.format("/topics/%s/%s", topicName,ebId))!=null){
				client.delete().forPath(String.format("/topics/%s/%s", topicName, ebId));
				logger.info("EdgeBroker:{} deleted its znode /topics/{}/{}", ebId, topicName, ebId);
			}

			// delete this EB's znode under: /lb/topics/topicName/ebId
			if(client.checkExists().forPath(String.format("/lb/topics/%s/%s",topicName,ebId))!=null){
				client.delete().deletingChildrenIfNeeded().forPath(String.format("/lb/topics/%s/%s", topicName, ebId));
				logger.info("EdgeBroker:{} deleted its znode /lb/topics/{}/{}", ebId, topicName, ebId);
			}

			/* check if there are other brokers hosting the topic, if not,
			 delete topic znode under: /topics/topicName 
			 and under: /lb/topics/topicName*/
			List<String> hosting_ebs = client.getChildren().forPath(String.format("/topics/%s", topicName));
			if (hosting_ebs.isEmpty()) {
				client.delete().forPath(String.format("/topics/%s", topicName));
				client.delete().deletingChildrenIfNeeded().forPath(String.format("/lb/topics/%s", topicName));
				logger.info("EdgeBroker:{} deleted topic:{} from the system as there are no hosting EBs", ebId,
						topicName);
			}

			// delete the subtree for this topic under this eb's znode: /eb/ebId/topicName/*
			client.delete().deletingChildrenIfNeeded().forPath(String.format("/eb/%s/%s", ebId, topicName));
			logger.info("EdgeBroker:{} deleted topic:{}'s sub-tree under its znode", ebId, topicName);
		}catch(Exception e){
			logger.error("EdgeBroker:{} caught exception:{}", ebId, e.getMessage());
		}
		
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
			int id=0;
			if(args.length==3){
				id= Integer.parseInt(args[2]);
			}

			//initialize EB 
			EdgeBroker eb= new EdgeBroker(zkConnector,
					ioThreads,id);
			Thread ebThread= new Thread(eb);

			//callback to handle SIGINT and SIGTERM
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					// shutdown EB's execution
					eb.shutdown();
					try {
						// wait until EB exits
						ebThread.join();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			});

			//start Eb
			ebThread.start();

		}catch(NumberFormatException e){
			e.printStackTrace();
		} 
	}

}
