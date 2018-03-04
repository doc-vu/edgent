package edu.vanderbilt.edgent.rebalancing;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import org.apache.curator.framework.CuratorFramework;
import org.zeromq.ZMQ.Socket;
import edu.vanderbilt.edgent.endpoints.Container;
import edu.vanderbilt.edgent.types.TopicCommandHelper;
import edu.vanderbilt.edgent.util.Commands;

public class AllPublisherReplication extends Rebalance {

	public AllPublisherReplication(String topicName, String currEbId, String currTopicConnector,
			boolean waitForDisconnection, CuratorFramework client, Socket topicControl,TopicCommandHelper topicCommandHelper) {
		super(Rebalance.LB_POLICY_ALL_PUB, topicName, currEbId, currTopicConnector, waitForDisconnection, client, topicControl,topicCommandHelper);
	}

	@Override
	public void populateRebalancedState() {
		/*
		 * Since, this is all publisher replication policy, 
		 * all currently connected publishers will need to 
		 * connect to all destination EBs.
		 * Also, none of the currently connected publishers will 
		 * disconnect from this EB for which the rebalancing action is happening.
		 * Hence, disconnectingPublishers list will be empty.
		 */
		for(String destEb: destEbTopicConnectors.keySet()){
			destEbMigratingPublishers.put(destEb, new ArrayList<String>(currentPublishers));
		}

		//Number of ebs participating in all-pub replication policy also includes the current EB 
		int numParticipatingEbs= destEbTopicConnectors.size() +1;
		//Number of subscribers per EB after rebalancing
		int numSubscribersPerEb= currentSubscribers.size()/numParticipatingEbs;
		//Remainder  
		int remainder= currentSubscribers.size()%numParticipatingEbs;
		int numSubscribersForCurrentEb= numSubscribersPerEb + remainder;
		int idx=numSubscribersForCurrentEb;
		for(String destEb: destEbTopicConnectors.keySet()){
			ArrayList<String> sub= new ArrayList<String>();
			for(int i=0;i<numSubscribersPerEb;i++){
				sub.add(currentSubscribers.get(idx));
				disconnectingSubscribers.add(currentSubscribers.get(idx));
				idx++;
			}
			destEbMigratingSubscribers.put(destEb,sub);
		}
		
	}

	@Override
	public void sendSubscriberMigrationControlMsg() {
		/*
		 * Only migrating subscribers must be notified 
		 * to connect to the destination EB.
		 */
		for(Entry<String, List<String>> pair: destEbMigratingSubscribers.entrySet()){
			String destEb= pair.getKey();
			String destTopicConnector= destEbTopicConnectors.get(destEb);
			for(String subscriberContainer: pair.getValue()){
				topicControl.sendMore(topicName.getBytes());
				topicControl.send(topicCommandHelper.serialize(Commands.TOPIC_LB_COMMAND,
						Commands.CONTAINER_CREATE_WORKER_COMMAND, subscriberContainer, destEb,
						destTopicConnector));
			}
		}
		
	}

	@Override
	public void sendPublisherMigrationControlMsg() {
		/*
		 * All connected publishers should connect
		 * to each of the new destination EBs. 
		 */
		for(Entry<String, String> pair: destEbTopicConnectors.entrySet()){
			topicControl.sendMore(topicName.getBytes());
			topicControl.send(topicCommandHelper.serialize(Commands.TOPIC_LB_COMMAND,
					Commands.CONTAINER_CREATE_WORKER_COMMAND,
					Container.ENDPOINT_TYPE_PUB, pair.getKey(), pair.getValue()));
		}
	}

	@Override
	public void sendSubDisconnectionControlMsg() {
		/*
		 *  Notify all disconnecting subscribers
		 *  to remove their connection to current EB and topicLocator.
		 *  Existing publishers won't disconnect
		 */
		for(String container: disconnectingSubscribers){
			topicControl.sendMore(topicName.getBytes());
			topicControl.send(topicCommandHelper.serialize(Commands.TOPIC_LB_COMMAND, 
					Commands.CONTAINER_DELETE_WORKER_COMMAND,
					container,currEbId,currTopicConnector));
		}
		
	}
	
	public void sendPubDisconnectionControlMsg(){
		//no-op
	}

}
