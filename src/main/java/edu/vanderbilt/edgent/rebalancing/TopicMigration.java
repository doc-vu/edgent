package edu.vanderbilt.edgent.rebalancing;

import java.util.ArrayList;
import java.util.Map.Entry;
import org.apache.curator.framework.CuratorFramework;
import org.zeromq.ZMQ.Socket;
import edu.vanderbilt.edgent.endpoints.Container;
import edu.vanderbilt.edgent.types.TopicCommandHelper;
import edu.vanderbilt.edgent.util.Commands;

public class TopicMigration extends Rebalance{

	public TopicMigration(String topicName, String currEbId, String currTopicConnector,
			boolean waitForDisconnection, CuratorFramework client, Socket topicControl,TopicCommandHelper topicCommandHelper) {
		super(Rebalance.LB_POLICY_MIGRATION, topicName, currEbId, currTopicConnector, waitForDisconnection, client, topicControl,topicCommandHelper);
	}

	@Override
	public void populateRebalancedState() {
		/*
		 * Since, this is topic migration policy, 
		 * all currently connected publishers and subscribers will need to 
		 * connect to the destination EB.
		 */
		for(String destEb: destEbTopicConnectors.keySet()){
			destEbMigratingSubscribers.put(destEb,new ArrayList<String>(currentSubscribers));
			destEbMigratingPublishers.put(destEb, new ArrayList<String>(currentPublishers));
		}
		//All subscribers will disconnect from the current EB
		for(String container: currentSubscribers){
			disconnectingSubscribers.add(container);
		}
		//All publishers will disconnect from the current EB
		for(String container: currentPublishers){
			disconnectingPublishers.add(container);
		}
	}

	@Override
	public void sendSubscriberMigrationControlMsg() {
		//All connected subscribers must connect to the new EB
		for(Entry<String, String> pair: destEbTopicConnectors.entrySet()){
			topicControl.sendMore(topicName.getBytes());
			topicControl.send(topicCommandHelper.serialize(Commands.TOPIC_LB_COMMAND,
					Commands.CONTAINER_CREATE_WORKER_COMMAND,
					Container.ENDPOINT_TYPE_SUB, pair.getKey(), pair.getValue()));
		}
	}

	@Override
	public void sendPublisherMigrationControlMsg() {
		//All connected publishers must connect to the new EB
		for(Entry<String, String> pair: destEbTopicConnectors.entrySet()){
			topicControl.sendMore(topicName.getBytes());
			topicControl.send(topicCommandHelper.serialize(Commands.TOPIC_LB_COMMAND,
					Commands.CONTAINER_CREATE_WORKER_COMMAND,
					Container.ENDPOINT_TYPE_PUB, pair.getKey(), pair.getValue()));
		}
	}

	@Override
	public void sendSubDisconnectionControlMsg() {
		//All endpoints(both publishers and subscribers) must disconnect from the current EB
		topicControl.sendMore(topicName.getBytes());
		topicControl.send(topicCommandHelper.serialize(Commands.TOPIC_LB_COMMAND, 
				Commands.CONTAINER_DELETE_WORKER_COMMAND,
				Container.ENDPOINT_TYPE_SUB,currEbId,currTopicConnector));
	}

	@Override
	public void sendPubDisconnectionControlMsg() {
		//All endpoints(both publishers and subscribers) must disconnect from the current EB
		topicControl.sendMore(topicName.getBytes());
		topicControl.send(topicCommandHelper.serialize(Commands.TOPIC_LB_COMMAND, 
				Commands.CONTAINER_DELETE_WORKER_COMMAND,
				Container.ENDPOINT_TYPE_PUB,currEbId,currTopicConnector));
	}

}
