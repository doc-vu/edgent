package edu.vanderbilt.edgent.rebalancing.test;

import java.util.Arrays;
import java.util.List;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import edu.vanderbilt.edgent.loadbalancer.LoadBalancer;

public class ReplicateTopics {
	private CuratorFramework client;

	public ReplicateTopics(String zkConnector){
		client= CuratorFrameworkFactory.newClient(zkConnector,
				new ExponentialBackoffRetry(1000, 3));
		client.start();
	}

	public void replicateTopics(String topicName,List<String> ebs,String replicationMode){
		try{
			Util.createTopic(client,topicName,ebs);
			client.setData().forPath(String.format("/topics/%s",topicName), replicationMode.getBytes());
			System.out.format("Set replication mode to:%s for topic:%s\n", replicationMode,topicName);
			CloseableUtils.closeQuietly(client);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	
	public static void main(String args[]){
		if(args.length<4){
			System.out.println("ReplicateTopics zkConnector topicName <comma separated list of ebs to replicate topic on> replicationMode(allSub|allPub)");
			return;
		}
		String zkConnector=args[0];
		String topicName=args[1];
		List<String> ebs= Arrays.asList(args[2].split(","));
		String replicationMode=args[3];
		if(!replicationMode.equals(LoadBalancer.REPLICATION_ALL_PUB) || 
				!replicationMode.equals(LoadBalancer.REPLICATION_ALL_SUB)){
			System.out.format("Replication mode:%s not recognized. Should either be:%s or %s\n",replicationMode,
					LoadBalancer.REPLICATION_ALL_PUB,LoadBalancer.REPLICATION_ALL_SUB);
			return;
		}
		new ReplicateTopics(zkConnector).replicateTopics(topicName,ebs,replicationMode);
	}

}
