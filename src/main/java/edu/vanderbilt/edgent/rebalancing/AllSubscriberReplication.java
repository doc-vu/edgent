package edu.vanderbilt.edgent.rebalancing;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import org.apache.curator.framework.CuratorFramework;
import org.zeromq.ZMQ.Socket;
import edu.vanderbilt.edgent.endpoints.Container;
import edu.vanderbilt.edgent.types.TopicCommandHelper;
import edu.vanderbilt.edgent.util.Commands;

public class AllSubscriberReplication extends Rebalance {

	public AllSubscriberReplication( String topicName, 
			String ebId, String topicConnector, boolean waitForDisconnection,
			CuratorFramework client, Socket topicControl,TopicCommandHelper topicCommandHelper) {
		super(Rebalance.LB_POLICY_ALL_SUB, topicName, ebId, topicConnector, waitForDisconnection,client, topicControl,topicCommandHelper);
	}

	@Override
	public void populateRebalancedState() {
		/*
		 * Since, this is all subscriber replication policy, 
		 * all currently connected subscribers will need to 
		 * connect to all destination EBs.
		 * Also, none of the currently connected subscribers will 
		 * disconnect from this EB for which the rebalancing action is happening.
		 * Hence, disconnectingSubscribers list will be empty.
		 */
		for(String destEb: destEbTopicConnectors.keySet()){
			destEbMigratingSubscribers.put(destEb, new ArrayList<String>(currentSubscribers));
		}

		//Number of ebs participating in all-sub replication policy also includes the current EB 
		int numParticipatingEbs= destEbTopicConnectors.size() +1;
		//Number of publishers per EB after rebalancing
		int numPublishersPerEb= currentPublishers.size()/numParticipatingEbs;
		//Remainder  
		int remainder= currentPublishers.size()%numParticipatingEbs;
		int numPublishersForCurrentEb= numPublishersPerEb + remainder;
		int idx=numPublishersForCurrentEb;
		for(String destEb: destEbTopicConnectors.keySet()){
			ArrayList<String> pub= new ArrayList<String>();
			for(int i=0;i<numPublishersPerEb;i++){
				pub.add(currentPublishers.get(idx));
				disconnectingPublishers.add(currentPublishers.get(idx));
				idx++;
			}
			destEbMigratingPublishers.put(destEb,pub);
		}
	}

	@Override
	public void sendSubscriberMigrationControlMsg() {
		/*
		 * All connected subscribers should connect
		 * to each of the new destination EBs. 
		 */
		for(Entry<String, String> pair: destEbTopicConnectors.entrySet()){
			topicControl.sendMore(topicName.getBytes());
			topicControl.send(topicCommandHelper.serialize(Commands.TOPIC_LB_COMMAND,
					Commands.CONTAINER_CREATE_WORKER_COMMAND,
					Container.ENDPOINT_TYPE_SUB, pair.getKey(), pair.getValue()));
		}
	}

	@Override
	public void sendPublisherMigrationControlMsg() {
		/*
		 * Only migrating publishers must be notified 
		 * to connect to the destination EB.
		 */
		for(Entry<String,List<String>> pair: destEbMigratingPublishers.entrySet()){
			String destEb= pair.getKey();
			String destTopicConnector= destEbTopicConnectors.get(destEb);
			for(String publisherContainer: pair.getValue()){
				topicControl.sendMore(topicName.getBytes());
				topicControl.send(topicCommandHelper.serialize(Commands.TOPIC_LB_COMMAND,
						Commands.CONTAINER_CREATE_WORKER_COMMAND, publisherContainer, destEb,
						destTopicConnector));
			}
		}
	}

	@Override 
	public void sendSubDisconnectionControlMsg(){
		//no-op
	}

	@Override
	public void sendPubDisconnectionControlMsg() {
		/*
		 *  Notify all disconnecting publishers 
		 *  to remove their connection to current EB and topicLocator.
		 *  Existing subscribers won't disconnect
		 */
		for(String container: disconnectingPublishers){
			topicControl.sendMore(topicName.getBytes());
			topicControl.send(topicCommandHelper.serialize(Commands.TOPIC_LB_COMMAND, 
					Commands.CONTAINER_DELETE_WORKER_COMMAND,
					container,currEbId,currTopicConnector));
		}
	}
	
}
