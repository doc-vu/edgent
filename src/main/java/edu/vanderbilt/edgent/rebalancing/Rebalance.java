package edu.vanderbilt.edgent.rebalancing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;

import edu.vanderbilt.edgent.types.TopicCommandHelper;

public abstract class Rebalance {
	public static final String LB_POLICY_MIGRATION="migration";
	public static final String LB_POLICY_ALL_SUB="allSub";
	public static final String LB_POLICY_ALL_PUB="allPub";

	//LB policy being enforced
	protected String lbPolicy;
	//topic for which reblancing is happening
	protected String topicName;
	//current EB's Id at which rebalancing is happening
	protected String currEbId;
	//Current EB's topic connector for topicName
	protected String currTopicConnector;

	//CuratorFramework client for ZK tree manipulation
	protected CuratorFramework client;
	//ZMQ.PUB socket to send LB commands for this topic
	protected ZMQ.Socket topicControl;
	
	private boolean waitForDisconnection;
	
	protected HashMap<String,String> destEbTopicConnectors;
	
	protected List<String> currentSubscribers;
	protected List<String> currentPublishers;

	protected HashMap<String,List<String>> destEbMigratingSubscribers;
	protected HashMap<String,List<String>> destEbMigratingPublishers;
	protected List<String> disconnectingSubscribers;
	protected List<String> disconnectingPublishers;

	protected HashMap<String,PathChildrenCache> destEbMigratingSubscribersListener;
	protected HashMap<String,PathChildrenCache> destEbMigratingPublishersListener;

	PathChildrenCache subscribersMigratedListener;
	PathChildrenCache publishersMigratedListener; 
	PathChildrenCache subscribersDisconnectedListener;
	PathChildrenCache publishersDisconnectedListener;

	protected TopicCommandHelper topicCommandHelper;
	protected DistributedBarrier subMigratedBarrier;
	protected DistributedBarrier pubMigratedBarrier; 
	private DistributedBarrier subDisconnectedBarrier;
	private DistributedBarrier pubDisconnectedBarrier;
	protected Logger logger;

	public Rebalance(String lbPolicy,String topicName,
			String currEbId,String currTopicConnector,boolean waitForDisconnection,
			CuratorFramework client, ZMQ.Socket topicControl,TopicCommandHelper topicCommandHelper){
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		this.lbPolicy=lbPolicy;
		this.topicName=topicName;
		this.currEbId=currEbId;
		this.currTopicConnector= currTopicConnector;
		this.waitForDisconnection=waitForDisconnection;
		this.client=client;
		this.topicControl=topicControl;
		this.topicCommandHelper=topicCommandHelper;
		
		destEbTopicConnectors= new HashMap<String,String>();
		
		destEbMigratingSubscribers= new HashMap<String,List<String>>();
		destEbMigratingPublishers= new HashMap<String,List<String>>();
		disconnectingSubscribers= new ArrayList<String>();
		disconnectingPublishers= new ArrayList<String>();
		
		destEbMigratingSubscribersListener= new HashMap<String,PathChildrenCache>();
		destEbMigratingPublishersListener= new HashMap<String,PathChildrenCache>();
	}
	
	public void execute(){
		try{
			// get destination Ebs and topic Connectors for this rebalancing action
			logger.debug("LbPolicy:{} will fetch destination Ebs and topic connectors",lbPolicy);
			getDestinationConnectors();

			/* get a list of connected subscribers at this EB, for which the
			 * current rebalancing action is happening
			 * */
			currentSubscribers = getConnectedSubscribers();
			logger.debug("LbPolicy:{} current list of subscribers:{}",lbPolicy,currentSubscribers);

			/* get a list of connected publishers at this EB, for which the 
			 * current rebalancing action is happening
			 */
			currentPublishers = getConnectedPublishers();
			logger.debug("LbPolicy:{} current list of publishers:{}",lbPolicy,currentPublishers);

			/* On the basis of lbPolicy, populate the migrated state of 
			 * endpoints
			 */
			logger.debug("LbPolicy:{} will construct post-rebalancing state information",lbPolicy,currentPublishers);
			populateRebalancedState();
			
			//set-up ZK paths for co-ordination
			logger.debug("LbPolicy:{} will setup ZK",lbPolicy);
			setupZkPaths();
		
			//install listeners for subscriber migration
			installListenersForSubscriberMigration();
			
			//install listeners for publisher migration
			installListenersForPublisherMigration();

			if(waitForDisconnection){
				installListenerForSubscriberDisconnection();
				installListenerForPublisherDisconnection();
			}
			// send topic LB control message to subscribers to connect to new endpoint
			sendSubscriberMigrationControlMsg();
			// wait for subscribers to migrate
			logger.info("LbPolicy:{} will wait until subscribers finish migrating", lbPolicy);
			subMigratedBarrier.waitOnBarrier();
			if(subscribersMigratedListener!=null){
				subscribersMigratedListener.close();
			}
			logger.info("LbPolicy:{} subscribers have migrated", lbPolicy);

			// send topic LB control message to publishers to connect to new endpoint
			sendPublisherMigrationControlMsg();
			// wait for publishers to migrate
			pubMigratedBarrier.waitOnBarrier();
			if(publishersMigratedListener!=null){
				publishersMigratedListener.close();
			}
			logger.info("LbPolicy:{} publishers have migrated", lbPolicy);
			
			
			Thread.sleep(5000);

			// send topic LB control message for endpoints to disconnect from current EB 
			sendSubDisconnectionControlMsg();
			if(subDisconnectedBarrier!=null){
				subDisconnectedBarrier.waitOnBarrier();
				subscribersDisconnectedListener.close();
				logger.info("LbPolicy:{} subscribers have disconnected", lbPolicy);
			}

			sendPubDisconnectionControlMsg();
			if(pubDisconnectedBarrier!=null){
				pubDisconnectedBarrier.waitOnBarrier();
				publishersDisconnectedListener.close();
				logger.info("LbPolicy:{} publishers have disconnected", lbPolicy);
			}
			logger.info("LbPolicy:{} finished executing",lbPolicy);
			client.delete().deletingChildrenIfNeeded().forPath(String.format("/lb/topics/%s/%s",topicName,currEbId));
		} catch (Exception e) {
			logger.error("LbPolicy:{} caught exception:{}", lbPolicy, e.getMessage());
		}
	}
	
	private void getDestinationConnectors() throws Exception{
		// fetch the destination topic connectors
		String listOfDestConnectors = new String(
				client.getData().forPath(String.format("/lb/topics/%s/%s/%s", topicName, currEbId, lbPolicy)));
		String[] connectors = listOfDestConnectors.split("\n");
		for (String connector : connectors) {
			String[] connectorParts = connector.split(";");
			String ebId = connectorParts[0];
			String topicConnector = connectorParts[1];
			destEbTopicConnectors.put(ebId, topicConnector);
			logger.debug("LbPolicy:{} destination EB:{} and topic connector:{}",lbPolicy,ebId, topicConnector);
		}

	}

	private List<String> getConnectedSubscribers()throws Exception {
		return client.getChildren().forPath(String.format("/eb/%s/%s/sub",currEbId,topicName));
	}
	
	private List<String> getConnectedPublishers() throws Exception{
		return client.getChildren().forPath(String.format("/eb/%s/%s/pub",currEbId,topicName));
	}

	private void setupZkPaths() throws Exception{
		// create barriers
		subMigratedBarrier = new DistributedBarrier(client,
				String.format("/lb/topics/%s/%s/%s/subMigrated", topicName, currEbId, lbPolicy));
		subMigratedBarrier.setBarrier();
		pubMigratedBarrier = new DistributedBarrier(client,
				String.format("/lb/topics/%s/%s/%s/pubMigrated", topicName, currEbId, lbPolicy));
		pubMigratedBarrier.setBarrier();
		
		logger.debug("LbPolicy:{} set subscriber and publisher migration barriers",lbPolicy);
	
		if(waitForDisconnection){
			subDisconnectedBarrier = new DistributedBarrier(client,
					String.format("/lb/topics/%s/%s/%s/subDisconnected", topicName, currEbId, lbPolicy));
			subDisconnectedBarrier.setBarrier();
			pubDisconnectedBarrier = new DistributedBarrier(client,
					String.format("/lb/topics/%s/%s/%s/pubDisconnected", topicName, currEbId, lbPolicy));
			pubDisconnectedBarrier.setBarrier();
			logger.debug("LbPolicy:{} set subscriber and publisher disconnected barriers",lbPolicy);
		}

		// create paths to monitor whether subscribers have moved to destination ebs
		client.create().forPath(String.format("/lb/topics/%s/%s/%s/subRebalancing", topicName, currEbId, lbPolicy));

		// create paths to monitor whether publishers have moved to destination ebs
		client.create().forPath(String.format("/lb/topics/%s/%s/%s/pubRebalancing", topicName, currEbId, lbPolicy));
		

		subscribersMigratedListener = new PathChildrenCache(client,
				String.format("/lb/topics/%s/%s/%s/subRebalancing", topicName, currEbId, lbPolicy), true);

		List<String> participatingSubscriberMigrationEbs= new ArrayList<String>();
		for(Entry<String, List<String>> pair: destEbMigratingSubscribers.entrySet()){
			String destEb=pair.getKey();
			List<String> migratingSubscribers= pair.getValue();
			if(!migratingSubscribers.isEmpty()){
				participatingSubscriberMigrationEbs.add(destEb);
			}
		}
		if(participatingSubscriberMigrationEbs.isEmpty()){
			subMigratedBarrier.removeBarrier();
		}else{
			subscribersMigratedListener.getListenable().addListener(new PathChildrenCacheListener() {

				@Override
				public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
					if (event.getType() == Type.CHILD_ADDED) {
						String path = event.getData().getPath();
						String[] pathParts = path.split("/");
						String joinedEb = pathParts[pathParts.length - 1];
						logger.info("LbPolicy:{} all migrating subscribers for eb:{} have joined", lbPolicy, joinedEb);
						destEbMigratingSubscribersListener.remove(joinedEb).close();
						participatingSubscriberMigrationEbs.remove(joinedEb);
						if (participatingSubscriberMigrationEbs.isEmpty()) {
							subMigratedBarrier.removeBarrier();
							logger.info("LbPolicy:{} opened subMigratedBarrier", lbPolicy);
						}
					}
				}
			});
			subscribersMigratedListener.start();
		}
		publishersMigratedListener = new PathChildrenCache(client,
				String.format("/lb/topics/%s/%s/%s/pubRebalancing", topicName, currEbId, lbPolicy), true);
		
		List<String> participatingPublisherMigrationEbs= new ArrayList<String>();
		for(Entry<String, List<String>> pair: destEbMigratingPublishers.entrySet()){
			String destEb=pair.getKey();
			List<String> migratingPublishers= pair.getValue();
			if(!migratingPublishers.isEmpty()){
				participatingPublisherMigrationEbs.add(destEb);
			}
		}
		if(participatingPublisherMigrationEbs.isEmpty()){
			pubMigratedBarrier.removeBarrier();
		}else{
			publishersMigratedListener.getListenable().addListener(new PathChildrenCacheListener() {

				@Override
				public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
					if (event.getType() == Type.CHILD_ADDED) {
						String path = event.getData().getPath();
						String[] pathParts = path.split("/");
						String joinedEb = pathParts[pathParts.length - 1];
						destEbMigratingPublishersListener.remove(joinedEb).close();
						participatingPublisherMigrationEbs.remove(joinedEb);
						if (participatingPublisherMigrationEbs.isEmpty()) {
							pubMigratedBarrier.removeBarrier();
						}
					}
				}
			});
			publishersMigratedListener.start();
		}

		logger.debug("LbPolicy:{} started listeners for subscriber and publisher migration",lbPolicy);
	}

	private void installListenersForSubscriberMigration() throws Exception {
		for (Entry<String, List<String>> pair : destEbMigratingSubscribers.entrySet()) {
			String destEb = pair.getKey();
			List<String> subscribers= pair.getValue();
			PathChildrenCache cache = new PathChildrenCache(client, String.format("/eb/%s/%s/sub", destEb, topicName),
					true);
			destEbMigratingSubscribersListener.put(destEb, cache);
			cache.getListenable().addListener(new MigratingSubscriberListener(destEb, subscribers));
			cache.start();
			logger.debug("LbPolicy:{} installed subscriber migration listener for eb:{} and subscriber list:{}",
					lbPolicy,destEb,subscribers);
		}
	}

	private void installListenersForPublisherMigration() throws Exception {
		for (Entry<String, List<String>> pair : destEbMigratingPublishers.entrySet()) {
			String destEb = pair.getKey();
			PathChildrenCache cache = new PathChildrenCache(client, String.format("/eb/%s/%s/pub", destEb, topicName),
					true);
			destEbMigratingPublishersListener.put(destEb, cache);
			cache.getListenable().addListener(new MigratingPublisherListener(destEb, pair.getValue()));
			cache.start();
		}
	}
	
	private void installListenerForSubscriberDisconnection() throws Exception{
		subscribersDisconnectedListener = new PathChildrenCache(client,
				String.format("/eb/%s/%s/sub", currEbId, topicName), true);
		if (!disconnectingSubscribers.isEmpty()) {
			subscribersDisconnectedListener.getListenable().addListener(new PathChildrenCacheListener() {

				@Override
				public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
					if (event.getType() == Type.CHILD_REMOVED) {
						// path: /eb/ebId/topicName/sub/containerId
						String path = event.getData().getPath();
						String[] pathParts = path.split("/");
						String containerId = pathParts[pathParts.length - 1];
						disconnectingSubscribers.remove(containerId);
						if (disconnectingSubscribers.isEmpty()) {
							subDisconnectedBarrier.removeBarrier();
						}
					}
				}
			});
			subscribersDisconnectedListener.start();
		} else {
			subDisconnectedBarrier.removeBarrier();
		}
	}

	private void installListenerForPublisherDisconnection() throws Exception{
		publishersDisconnectedListener= new PathChildrenCache(client,
				String.format("/eb/%s/%s/pub",currEbId,topicName),true);
		if(!disconnectingPublishers.isEmpty()){
			publishersDisconnectedListener.getListenable().addListener(new PathChildrenCacheListener(){

				@Override
				public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
					if(event.getType()==Type.CHILD_REMOVED){
						//path: /eb/ebId/topicName/pub/containerId
						String path= event.getData().getPath();
						String[] pathParts= path.split("/");
						String containerId= pathParts[pathParts.length-1];
						disconnectingPublishers.remove(containerId);
						if(disconnectingPublishers.isEmpty()){
							pubDisconnectedBarrier.removeBarrier();
						}
					}
				}
			});
			publishersDisconnectedListener.start();
		}else{
			pubDisconnectedBarrier.removeBarrier();
		}
	}

	public abstract void populateRebalancedState();	
	public abstract void sendSubscriberMigrationControlMsg();
	public abstract void sendPublisherMigrationControlMsg();
	public abstract void sendSubDisconnectionControlMsg();
	public abstract void sendPubDisconnectionControlMsg();
	
 	private class MigratingSubscriberListener implements PathChildrenCacheListener{
		private String destEb;
		private List<String> migratingSubscribers;
		public MigratingSubscriberListener(String destEb, List<String> migratingSubscribers){
			this.destEb= destEb;
			this.migratingSubscribers=migratingSubscribers;
			logger.debug("LbPolicy:{} initalized MigratingSubscriberListener for destEb:{} and migratingSubscribers:{}",
					lbPolicy,destEb,migratingSubscribers);
		}

		@Override
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
			if(event.getType()==Type.CHILD_ADDED){
				//path: /eb/destEb/topicName/sub/containerId
				String path=event.getData().getPath();
				String[] pathParts=path.split("/");
				String containerId= pathParts[pathParts.length -1];
				logger.debug("LbPolicy:{} endpoint:{} joined",lbPolicy,containerId);
				boolean result=migratingSubscribers.remove(containerId);
				logger.debug("LbPolicy:{} removal result:{}. length of migratingSubscribers:{}. isEmpty:{}",
						lbPolicy,result,migratingSubscribers.size(),migratingSubscribers.isEmpty());

				if(migratingSubscribers.isEmpty()){
					logger.info("LbPolicy:{} subscribers for eb:{} have joined",lbPolicy,destEb);
					client.create().forPath(String.format("/lb/topics/%s/%s/%s/subRebalancing/%s",
							topicName,currEbId,lbPolicy,destEb));
				}
			}
		}
	}


	private class MigratingPublisherListener implements PathChildrenCacheListener{
		private String destEb;
		private List<String> migratingPublishers;

		public MigratingPublisherListener(String destEb, List<String> migratingPublishers){
			this.destEb=destEb;
			this.migratingPublishers=migratingPublishers;
		}

		@Override
		public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
			if(event.getType()==Type.CHILD_ADDED){
				//path: /eb/destEb/topicName/pub/containerId
				String path=event.getData().getPath();
				String[] pathParts=path.split("/");
				String containerId= pathParts[pathParts.length -1];
				migratingPublishers.remove(containerId);
				if(migratingPublishers.isEmpty()){
					client.create().forPath(String.format("/lb/topics/%s/%s/%s/pubRebalancing/%s",
							topicName,currEbId,lbPolicy,destEb));
				}
			}
		}
	}
	
	
}
