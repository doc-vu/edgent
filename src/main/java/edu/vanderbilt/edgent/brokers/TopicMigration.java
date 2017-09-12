package edu.vanderbilt.edgent.brokers;

import java.util.HashSet;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.zeromq.ZMQ;
import edu.vanderbilt.edgent.endpoints.Container;
import edu.vanderbilt.edgent.types.TopicCommandHelper;
import edu.vanderbilt.edgent.util.Commands;

public class TopicMigration {
	public static final String LB_POLICY="migration";
	private String topicName;
	private String sourceEb;
	private String currTopicConnector;
	private String destEb;
	private String newTopicConnector;
	private CuratorFramework client;
	private ZMQ.Socket topicControl;

	private HashSet<String> subscribers;
	private HashSet<String> oldSubscribers;
	private HashSet<String> publishers;
	private HashSet<String> oldPublishers;

	private DistributedBarrier subMigrated;
	private DistributedBarrier subExited;
	private DistributedBarrier pubMigrated; 
	private DistributedBarrier pubExited;
	
	private PathChildrenCache subJoined;
	private PathChildrenCache pubJoined;
	private PathChildrenCache subLeft;
	private PathChildrenCache pubLeft;

	public TopicMigration(String topicName, String ebId, String topicConnector,
			CuratorFramework client,ZMQ.Socket topicControl){
		this.topicName=topicName;
		this.sourceEb=ebId;
		this.currTopicConnector=topicConnector;
		this.client=client;
		this.topicControl=topicControl;
	}
	
	public void execute(){
		try {
			//remove this eb's znode under: /topics/topicName/ebId so that new endpoints don't connect here
			client.delete().forPath(String.format("/topics/%s/%s", topicName,sourceEb));
			setupZk();
			//send control message for subscribers to also connect to new topic connector
			topicControl.sendMore(topicName.getBytes());
			topicControl.send(TopicCommandHelper.serialize(Topic.TOPIC_LB_COMMAND, 
					Commands.CONTAINER_CREATE_WORKER_COMMAND,Container.FOR_ALL_CONTAINERS,
					destEb,newTopicConnector));

			//wait until subscribers are connected to new EB
			if(subMigrated!=null){
				subMigrated.waitOnBarrier();
				subJoined.close();
			}
		
			//wait until publishers are connected to new EB
			if(pubMigrated!=null){
				pubMigrated.waitOnBarrier();
				pubJoined.close();
			}
		
			//send control message to disconnect all endpoints from old EB
			topicControl.sendMore(topicName.getBytes());
			topicControl.send(TopicCommandHelper.serialize(Topic.TOPIC_LB_COMMAND, Commands.CONTAINER_DELETE_WORKER_COMMAND,
					Container.FOR_ALL_CONTAINERS,sourceEb,currTopicConnector));
			
			//wait until old subscriber connections are removed
			if(subExited!=null){
				subExited.waitOnBarrier();
				subLeft.close();
			}

			//wait until old publisher connections are removed
			if(pubExited!=null){
				pubExited.waitOnBarrier();
				pubLeft.close();
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	public void setupZk(){
		try{
			//get destination topic connector
			String ebIdAndTopicConnector=new String(client.getData().
					forPath(String.format("/lb/topics/%s/%s/%s",
							topicName,sourceEb,LB_POLICY)));
			String parts[]= ebIdAndTopicConnector.split(";");
			destEb=parts[0];
			newTopicConnector=parts[1];
			
			//get all subscribers for this topic connected to sourceEb
			subscribers= new HashSet<String>(client.getChildren().forPath(String.format("/eb/%s/%s/sub",sourceEb,topicName)));
			if(!subscribers.isEmpty()){
				oldSubscribers= new HashSet<String>();
				subMigrated = new DistributedBarrier(client,
						String.format("/lb/topics/%s/%s/%s/subMigrated", topicName, sourceEb, LB_POLICY));
				subMigrated.setBarrier();
				subJoined = new PathChildrenCache(client, String.format("/eb/%s/%s/sub", destEb, topicName), true);
				subJoined.getListenable().addListener(new PathChildrenCacheListener() {
					@Override
					public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
						if (event.getType() == Type.CHILD_ADDED) {
							String znodePath = event.getData().getPath();
							String[] parts = znodePath.split("/");
							String subId = parts[parts.length - 1];
							subscribers.remove(subId);
							oldSubscribers.add(subId);
							if (subscribers.isEmpty()) {
								subMigrated.removeBarrier();
							}
						}
					}
				});
				subJoined.start();
				subExited = new DistributedBarrier(client,
						String.format("/lb/topics/%s/%s/%s/subExited", topicName, sourceEb, LB_POLICY));
				subExited.setBarrier();
				subLeft = new PathChildrenCache(client, String.format("/eb/%s/%s/sub", sourceEb, topicName), true);
				subLeft.getListenable().addListener(new PathChildrenCacheListener() {

					@Override
					public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
						if (event.getType() == Type.CHILD_REMOVED) {
							String znodePath = event.getData().getPath();
							String[] parts = znodePath.split("/");
							String subId = parts[parts.length - 1];
							oldSubscribers.remove(subId);
							if (oldSubscribers.isEmpty()) {
								subExited.removeBarrier();
							}
						}
					}

				});
				subLeft.start();
			}
			//get all publishers for this topic connected to sourceEb
			publishers= new HashSet<String>(client.getChildren().forPath(String.format("/eb/%s/%s/pub",sourceEb,topicName)));
			if(!publishers.isEmpty()){
				oldPublishers = new HashSet<String>();

				pubMigrated = new DistributedBarrier(client,
						String.format("/lb/topics/%s/%s/%s/pubMigrated", topicName, sourceEb, LB_POLICY));
				pubMigrated.setBarrier();
				pubJoined = new PathChildrenCache(client, String.format("/eb/%s/%s/pub", destEb, topicName), true);
				pubJoined.getListenable().addListener(new PathChildrenCacheListener() {
					@Override
					public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
						if (event.getType() == Type.CHILD_ADDED) {
							String znodePath = event.getData().getPath();
							String[] parts = znodePath.split("/");
							String pubId = parts[parts.length - 1];
							publishers.remove(pubId);
							oldPublishers.add(pubId);
							if (publishers.isEmpty()) {
								pubMigrated.removeBarrier();
							}
						}
					}
				});
				pubJoined.start();

				pubExited = new DistributedBarrier(client,
						String.format("/lb/topics/%s/%s/%s/pubExited", topicName, sourceEb, LB_POLICY));
				pubExited.setBarrier();

				pubLeft = new PathChildrenCache(client, String.format("/eb/%s/%s/pub", sourceEb, topicName), true);
				pubLeft.getListenable().addListener(new PathChildrenCacheListener() {

					@Override
					public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
						if (event.getType() == Type.CHILD_REMOVED) {
							String znodePath = event.getData().getPath();
							String[] parts = znodePath.split("/");
							String pubId = parts[parts.length - 1];
							oldPublishers.remove(pubId);
							if (oldPublishers.isEmpty()) {
								pubExited.removeBarrier();
							}
						}
					}
				});
				pubLeft.start();
			}

		}catch(Exception e){
			e.printStackTrace();
		}
	}

}
