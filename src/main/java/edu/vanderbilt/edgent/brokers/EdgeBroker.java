package edu.vanderbilt.edgent.brokers;

import java.util.HashMap;
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
	private ZMQ.Context context;
	//Map to maintain hosted topics
	private Map<String,Topic> hostedTopics;
	//Curator client for ZK connection
	private CuratorFramework client;

	private Logger logger;
	
	public EdgeBroker(int regionId,String zkConnector, int ioThreads){
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		ebId=String.format("EB-%d-%s",regionId,UtilMethods.ipAddress());

		//create ZMQ Context
		context= ZMQ.context(ioThreads);
		hostedTopics= new HashMap<String,Topic>();
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
			PathChildrenCache cache= new PathChildrenCache(client,"/eb",true);
			cache.getListenable().addListener(new PathChildrenCacheListener(){
				@Override
				public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
					//new topic was created and assigned to this EB 
					if(event.getType()==Type.CHILD_ADDED){
						String path=event.getData().getPath();
						logger.debug("EB:{} was assigned topic:{}",ebId,path );
					}
				}
			});

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
			Topic topic= new Topic(topicName,context,receivePort,sendPort);
			hostedTopics.put(topicName,topic);
			new Thread(topic).start();
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
		logger.debug("EdgeBroker:{} deleting topic:{}",ebId,topicName);
		if(hostedTopics.containsKey(topicName))
		{
			Topic topic=hostedTopics.remove(topicName);
			topic.stop();
			//TODO release port numbers
			logger.info("EdgeBroker:{} deleted topic:{}", ebId,topicName);

		}else{
			logger.error("EdgeBroker:{} topic:{} does not exist",ebId,topicName);
		}
	}
	
	public void close()
	{
		context.close();
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
			EdgeBroker eb= new EdgeBroker(regionId,zkConnector,ioThreads);
			//eb.start();
			eb.createTopic("t1", 5555, 6666);
			Thread.sleep(5000);
			eb.deleteTopic("t1");
			//eb.close();

		}catch(NumberFormatException e){
			e.printStackTrace();
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
