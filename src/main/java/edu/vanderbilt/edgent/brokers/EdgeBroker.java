package edu.vanderbilt.edgent.brokers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.zookeeper.CreateMode;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import edu.vanderbilt.edgent.util.PortList;
import edu.vanderbilt.edgent.util.UtilMethods;
/**
 * EdgeBroker managing local/intra-region data dissemination concerns.
 * @author kharesp
 */
public class EdgeBroker {
	private String ebId;

	//List of avaiable ports
	private PortList ports;
	//ZMQ Context
	private ZContext context;
	private ZMQ.Socket socket;
	//Map to maintain hosted topics
	private Map<String,Topic> hostedTopics;
	private List<Thread> topicThreads;
	//Curator client for ZK connection
	private CuratorFramework client;
	private PathChildrenCache topicAssignmentListener;
	

	private Logger logger;
	
	public EdgeBroker(int regionId,String zkConnector,ZContext context){
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		ebId=String.format("EB-%d-%s",regionId,UtilMethods.ipAddress());

		//create ZMQ Context
		this.context=context;
		socket=context.createSocket(ZMQ.PUB);
		socket.bind(String.format("tcp://*:%d",Topic.TOPIC_CONTROL_PORT));

		hostedTopics= new HashMap<String,Topic>();
		topicThreads= new ArrayList<Thread>();
		ports= PortList.getInstance();
	
		//initialize curator client for ZK connection
		client=CuratorFrameworkFactory.newClient(zkConnector,
				new ExponentialBackoffRetry(1000, 3));
		client.start();
		
		logger.debug("Initialized EdgeBroker:{}",ebId);
	}
	
	public void start()
	{
		try{
			//create eb znode under /eb/eb1
			client.create().forPath(String.format("/eb/%s",ebId));
			//register child listener for the created znode
			topicAssignmentListener= new PathChildrenCache(client,String.format("/eb/%s",ebId),true);
			topicAssignmentListener.getListenable().addListener(new PathChildrenCacheListener(){
				@Override
				public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
					//new topic was created and assigned to this EB 
					if(event.getType()==Type.CHILD_ADDED){
						String path=event.getData().getPath();
						String topic=path.split("/")[3];
						logger.debug("EB:{} was assigned topic:{}",ebId,topic);
						createTopic(topic,ports.acquire(),ports.acquire());
					}
				}
			});
			topicAssignmentListener.start();

		}catch(Exception e){
			logger.error("EB:{} caught exception:{}",
					ebId,e.getMessage());
		}
	}
	
	/**
	 * Creates a topic which will be hosted on this EB. 
	 * @param topicName 
	 */
	public void createTopic(String topicName,int receivePort, int sendPort){
		logger.debug("EdgeBroker:{} creating topic:{}",ebId,topicName);
		if(!hostedTopics.containsKey(topicName)){
			//TODO acquire port numbers from portList
			Topic topic= new Topic(topicName,ZContext.shadow(context),
					receivePort,sendPort);
			hostedTopics.put(topicName,topic);
			Thread topicThread=new Thread(topic);
			topicThreads.add(topicThread);
			topicThread.start();
			logger.info("EdgeBroker:{} created topic:{}",ebId,topicName);
		}else{
			logger.error("EdgeBroker:{} topic:{} already exists",ebId,topicName);
		}
	}

	/**
	 * Deletes a topic hosted on this EB.
	 * @param topicName
	 */
	public void deleteTopic(String topicName){
		System.out.println("DELETE TOPIC CALLED");
		logger.debug("EdgeBroker:{} deleting topic:{}",ebId,topicName);
		if(hostedTopics.containsKey(topicName))
		{
			Topic topic=hostedTopics.remove(topicName);
			socket.send(String.format("%s %s",topicName,Topic.TOPIC_DELETE_COMMAND));
			//TODO release port numbers
			ports.release(topic.receivePort());
			ports.release(topic.sendPort());
			logger.info("EdgeBroker:{} deleted topic:{}", ebId,topicName);

		}else{
			logger.error("EdgeBroker:{} topic:{} does not exist",ebId,topicName);
		}
	}
	
	public void close()
	{
		for(String topic: hostedTopics.keySet()){
			deleteTopic(topic);
		}

		try{
			for(Thread t: topicThreads){
				t.join();
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}
		socket.close();
		context.destroy();
		
		try {
			topicAssignmentListener.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		CloseableUtils.closeQuietly(client);
	}
	

	public static void main(String args[]){
		if(args.length<3){
			System.out.println("Usage: EdgeBroker regionId zkConnector ioThreads");
			return;
		}
		try{
			int regionId=Integer.parseInt(args[0]);
			String zkConnector=args[1];
			int ioThreads=Integer.parseInt(args[2]);
			ZContext context= new ZContext(ioThreads);
			EdgeBroker eb= new EdgeBroker(regionId,zkConnector,
					ZContext.shadow(context));

			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					eb.close();
					context.destroy();
				}
			});
			
			eb.start();
			while(true){
				Thread.sleep(10000);
			}

		}catch(NumberFormatException  | InterruptedException e){
			e.printStackTrace();
		} 
	}

}
