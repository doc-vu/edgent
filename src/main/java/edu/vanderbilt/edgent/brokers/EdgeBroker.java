package edu.vanderbilt.edgent.brokers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
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
import org.apache.zookeeper.data.Stat;
import org.zeromq.ZMQ;
import edu.vanderbilt.edgent.util.PortList;
import edu.vanderbilt.edgent.util.UtilMethods;
/**
 * EdgeBroker managing local/intra-region data dissemination concerns.
 * @author kharesp
 */
public class EdgeBroker implements Runnable{
	//Periodic interval after which pruning thread is scheduled
	public static final int PERIODIC_PRUNING_INTERVAL_SEC=120;
	//Time interval after which an unused topic is removed
	public static final int TOPIC_EXPIRY_PERIOD_SEC=20;

	//Port at which hosting broker issues topic control messages
	public static final int TOPIC_CONTROL_PORT=4993;
	//Topic control messages
	public static final String TOPIC_DELETE_COMMAND="delete";
	public static final String TOPIC_CREATION_COMMAND="create";
	public static final String TOPIC_LB_COMMAND="lb";
	public static final String EXIT_COMMAND="exit";

	//List of avaiable ports
	private PortList ports;
	//ZMQ Context
	private ZMQ.Context context;
	//ZMQ PUB socket to issue topic
	//control messages for hosted topics
	private ZMQ.Socket topicControl;

	/* HashTables to maintain hosted topics and 
	 * corresponding topic Threads. All HashTable operations
	 * are synchronized for consistency during concurrent access:e.g.,
	 * ZK callback will insert a new topic, while a 
	 * periodic clean-up thread will remove a topic with no 
	 * interested end-points. 
	 */
	private Hashtable<String,Topic> hostedTopics;
	private Hashtable<String,Thread> topicThreads;
	private HashMap<String,PathChildrenCache> topicLevelLbActions;

	//Curator client for ZK connection
	private CuratorFramework client;
	//ZK path children listener, to listen for topic assignment to this broker
	private PathChildrenCache topicAssignmentListener;

	//Executor to run the topic clean-up task periodically
	private ScheduledExecutorService scheduler;
	
	private LinkedBlockingQueue<String> queue;
	
	//This broker's ipAddress,regionId and Id
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
		hostedTopics= new Hashtable<String,Topic>();
		topicThreads= new Hashtable<String,Thread>();
		topicLevelLbActions=new HashMap<String,PathChildrenCache>();
		
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


	@Override
	public void run()
	{
		try{
			// create ZMQ Context and initialize ZMQ topic control socket
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
						String topic=path.split("/")[3];
						logger.info("EB:{} was assigned topic:{}",ebId,topic);

						queue.add(String.format("%s,%s,%d,%d,%d",
								TOPIC_CREATION_COMMAND,topic,
								ports.acquire(),ports.acquire(),ports.acquire()));
					}
					
				}
			});
			topicAssignmentListener.start();

			// schedule periodic pruning of topics
			scheduler = Executors.newScheduledThreadPool(1);
			scheduler.scheduleWithFixedDelay(new Prune(), 
					PERIODIC_PRUNING_INTERVAL_SEC, PERIODIC_PRUNING_INTERVAL_SEC,
					TimeUnit.SECONDS);

			boolean listen=true;
			while(listen && !Thread.currentThread().isInterrupted()){
				String command=queue.take();
				System.out.println("EB loop extracted command: "+ command);
				String[] args=command.split(",");
				switch(args[0]){
				case TOPIC_CREATION_COMMAND:
					String topic=args[1];
					int receivePort=Integer.parseInt(args[2]);
					int sendPort=Integer.parseInt(args[3]);
					int controlPort=Integer.parseInt(args[4]);
					createTopic(topic,receivePort,sendPort,controlPort);
					break;
				case TOPIC_DELETE_COMMAND:
					deleteTopic(args[1]);
					break;
				case EXIT_COMMAND:
					listen=false;
					System.out.println("set listen to false");
					break;
				case TOPIC_LB_COMMAND:
					testLb(args[1]);
					break;
				default:
						break;
				};
			}
			System.out.println("EB listener loop exited. Calling cleanup");
			cleanup();

		}catch(Exception e){
			logger.error("EB:{} caught exception:{}",
					ebId,e.getMessage());
			cleanup();
		}
	}
	
	/**
	 * Creates a topic which will be hosted on this EB. 
	 * First starts the topic thread and then creates this EB's znode 
	 * under /topics/topicName/eb to signify that the newly created topic
	 * is hosted on this EB.
	 * @param topicName 
	 */
	public void createTopic(String topicName,int receivePort, int sendPort,int controlPort){
		logger.debug("EdgeBroker:{} will create topic:{}",ebId,topicName);

		if(!hostedTopics.containsKey(topicName)){
			//Instantiate topic
			Topic topic= new Topic(topicName,context,
					receivePort,sendPort,controlPort);
			//Add created topic instance to hostedTopics map
			hostedTopics.put(topicName,topic);
			
			//Start the topic thread
			Thread topicThread=new Thread(topic);
			topicThreads.put(topicName,topicThread);
			topicThread.start();
			logger.debug("EdgeBroker:{} topic:{} thread started",ebId,topicName);


			try {
				// register listener for receiving topic level LB directives for
				// this topic
				PathChildrenCache cache = new PathChildrenCache(client, String.format("/lb/topics/%s", topicName),
						true);
				topicLevelLbActions.put(topicName, cache);
				cache.getListenable().addListener(new PathChildrenCacheListener() {
					@Override
					public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
						if(event.getType()==Type.CHILD_ADDED){
							queue.add(String.format("%s,%s",TOPIC_LB_COMMAND,topicName));
						}
					}
				});
				cache.start();
				// create /topics/topicName/ebId
				client.create().creatingParentsIfNeeded().forPath(String.format("/topics/%s/%s", topicName, ebId),
						String.format("%s,%d,%d,%d", ipAddress, receivePort, sendPort, controlPort).getBytes());
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
	 * @throws InterruptedException 
	 */
	public void deleteTopic(String topicName) {
		if(hostedTopics.containsKey(topicName))
		{
			//remove Topic and topic thread from hostedTopics and topicThreads map
			Topic topic= hostedTopics.remove(topicName);
			Thread topicThread= topicThreads.remove(topicName);
			PathChildrenCache topicLbCache= topicLevelLbActions.remove(topicName);
			try {
				topicLbCache.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			deleteTopic(topicName, topic,topicThread);
			logger.info("EdgeBroker:{} deleted topic:{}", ebId,topicName);
		}else{
			logger.error("EdgeBroker:{} topic:{} does not exist",ebId,topicName);
		}
	}

	private void testLb(String topic){
		for(int i=0;i<1000;i++){
			topicControl.send(String.format("%s %s",topic,EdgeBroker.TOPIC_LB_COMMAND));
		}
	}
	/**
	 * Deletes all topics hosted on this EB
	 * @throws InterruptedException 
	 */
	public void deleteAllTopics()
	{
		/* HashTable's iterator is fail-fast. We can't iterate and modify the 
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

			logger.debug("EdgeBroker:{} will delete topic:{}",ebId,topicName);
			deleteTopic(topicName,topic,topicThread);
			logger.info("EdgeBroker:{} deleted topic:{}",ebId,topicName);
		}
	}

	private void deleteTopic(String topicName, Topic topic, Thread tThread) {
		try {
			System.out.println("delte topic called!!!");
			// delete this EB's znode under /topics/topic
			client.delete().forPath(String.format("/topics/%s/%s", topicName, ebId));
			logger.debug("EdgeBroker:{} deleted its znode under /topics/{} path",ebId,topicName);


			//check if there are other brokers hosting the topic, if not, delete topic znode
			List<String> hosting_ebs= client.getChildren().forPath(String.format("/topics/%s", topicName));
			if(hosting_ebs.isEmpty()){
				client.delete().forPath(String.format("/topics/%s", topicName));
				client.delete().forPath(String.format("/lb/topics/%s", topicName));
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
			System.out.println("will join on topic thread");
			tThread.join();
			logger.debug("EdgeBroker:{} topic:{} thread has exited",
					ebId,topicName);
			System.out.println("join exited");

			// release receiver and sender port numbers of this topic for reuse
			ports.release(topic.receivePort());
			ports.release(topic.sendPort());
		} catch (Exception e) {
			logger.error("EdgeBroker:{} caught exception:{}", ebId, e.getMessage());
		}
	}

	/**
	 * Performs clean up before exiting
	 */
	public void cleanup() 
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
			client.delete().forPath(String.format("/eb/%s",ebId));
			logger.debug("EdgeBroker:{} deleted its znode under /eb",ebId);
		} catch (Exception e) {
			logger.error("EdgeBroker:{} caught exception:{}",
					ebId,e.getMessage());
		}
		
		CloseableUtils.closeQuietly(client);
		logger.debug("EdgeBroker:{} closed ZK connection",ebId);
		logger.info("EdgeBroker:{} exited",ebId);
	}
	
	public void shutdown(){
		queue.add(EXIT_COMMAND);
	}
	
	private class Prune implements Runnable{
		private Logger logger;
		public Prune(){
			logger= LogManager.getLogger(this.getClass().getSimpleName());
		}
		@Override
		public void run() {
			try {
				logger.info("Periodic pruning scheduled on thread:{}",Thread.currentThread().getName());
				//Acquire list of hosted topics at this broker
				List<String> topics=client.getChildren().forPath(String.format("/eb/%s",ebId));
				//For each topic, check if any interested endpoint exists
				for(String topic: topics){
					logger.debug("Periodic pruning thread:{}: assessing topic:{}",
							Thread.currentThread().getName(),topic);
						List<String> publishers= client.getChildren().forPath(String.format("/eb/%s/%s/pub",ebId,topic));
						if (publishers.isEmpty()) {
							List<String> subscribers = client.getChildren()
									.forPath(String.format("/eb/%s/%s/sub", ebId, topic));
							if (subscribers.isEmpty()) {
								//both publishers and subscribers for this topic don't exist
								// delete topic if it is older than set-timeout 
								Stat topicStat=client.checkExists().
										forPath(String.format("/topics/%s",topic));
								long elapsed_milisec=System.currentTimeMillis()-topicStat.getCtime();
								logger.debug("Periodic pruning thread:{} topic:{} was created at:{} elapsed time:{}",
										Thread.currentThread().getName(),topic,topicStat.getCtime(),elapsed_milisec/1000);
								if(elapsed_milisec/1000 > TOPIC_EXPIRY_PERIOD_SEC){
									logger.info("Periodic pruning thread:{}: "
												+ "will delete topic:{}",
										Thread.currentThread().getName(), topic);
									queue.add(String.format("%s,%s", TOPIC_DELETE_COMMAND,topic));
								}
							}
						}
				}

			} catch (Exception e) {
				logger.error("Periodic pruning thread:{} caught exception:{}",Thread.currentThread().getName(),
						e.getMessage());
			}
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

			//initialize EB
			EdgeBroker eb= new EdgeBroker(zkConnector,
					ioThreads);
			Thread ebThread= new Thread(eb);

			//callback to handle SIGINT and SIGTERM
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					eb.shutdown();
					try{
						ebThread.join();
					}catch(InterruptedException e){}
				}
			});

			ebThread.start();

		}catch(NumberFormatException e){
			e.printStackTrace();
		} 
	}

}
