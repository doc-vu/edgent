package edu.vanderbilt.edgent.rebalancing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

public class Test implements Runnable {
	private AtomicBoolean listening;
	private CuratorFramework client;
	private final String topicName="t1";
	private final String sourceEb="EB-0-127.0.0.1-1";
	private final List<String> destEbs= Arrays.asList("EB-0-127.0.0.1-2","EB-0-127.0.0.1-3","EB-0-127.0.0.1-4");

	public Test(String zkConnector){
		listening= new AtomicBoolean(true);
		client= CuratorFrameworkFactory.newClient(zkConnector,
				new ExponentialBackoffRetry(1000, 3));
		client.start();
	}

	@Override
	public void run(){
		//Install PathChildren
		PathChildrenCache cache = new PathChildrenCache(client, "/lbtest", true);
		cache.getListenable().addListener(new PathChildrenCacheListener(){

			@Override
			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
				if(event.getType()==Type.CHILD_ADDED){
					String znodePath= event.getData().getPath();
					String[] parts= znodePath.split("/");
					String lbPolicy= parts[2];
					if(lbPolicy.equals(Rebalance.LB_POLICY_ALL_SUB)){
						rebalance(sourceEb,Rebalance.LB_POLICY_ALL_SUB,destEbs);
					}else if(lbPolicy.equals(Rebalance.LB_POLICY_ALL_PUB)){
						rebalance(sourceEb,Rebalance.LB_POLICY_ALL_PUB, destEbs);
					}else if(lbPolicy.equals(Rebalance.LB_POLICY_MIGRATION)){
						rebalance(sourceEb,Rebalance.LB_POLICY_MIGRATION,Arrays.asList(destEbs.get(0)));
					}
					else{
						System.out.format("LB policy:%s not recognized\n",lbPolicy);
					}
				}
			}
		});
		try {
			cache.start();
			while(listening.get()){
				Thread.sleep(5000);
			}
			cache.close();
			CloseableUtils.closeQuietly(client);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	public void stop(){
		listening.set(false);
	}
	
	private void rebalance(String sourceEb, String lbPolicy, List<String> ebs){
		try{
			
			/*Create topic on all destination Ebs. A new list is created and passed, 
			 * as it is modified by createTopic function
			 */
			createTopic(new ArrayList<String>(ebs));
		
			//Get string representation of all destination topic connectors 
			String topicConnectorString=getTopicConnectorString(ebs);
			
			//create znode under /lb path to trigger lb action
			client.create().forPath(String.format("/lb/topics/%s/%s/%s",topicName,sourceEb,lbPolicy),
					topicConnectorString.getBytes());

		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private void createTopic(List<String> ebs) throws Exception{
		// create topic topicName on all destination Ebs
		for (String eb : ebs) {
			client.create().forPath(String.format("/eb/%s/%s", eb,topicName));
			System.out.println(String.format("created topic:%s for destination eb:%s", topicName, eb));
		}
		// latch to wait on until topic topicName is allocated on all
		// destination Ebs
		CountDownLatch latch = new CountDownLatch(1);

		// install listener to monitor topic allocation on destination ebs
		PathChildrenCache cache = new PathChildrenCache(client, String.format("/topics/%s", topicName), false);
		cache.getListenable().addListener(new PathChildrenCacheListener() {
			@Override
			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
				if (event.getType().equals(Type.CHILD_ADDED)) {
					// znodePath: /topics/topicName/ebId
					String znodePath = event.getData().getPath();
					String[] parts = znodePath.split("/");
					String eb = parts[parts.length - 1];
					ebs.remove(eb);
					if (ebs.isEmpty()) {
						latch.countDown();
					}
				}
			}
		});
		cache.start();

		System.out.println(String.format("Will wait until topic:%s is allocted on all dest ebs:%s",
				topicName, ebs.toString()));
		latch.await();
		cache.close();
		System.out.println(String.format("Topic:%s was allocated on all dest ebs",
				topicName));
	}
	
	private String getTopicConnectorString(List<String> destEbs) throws Exception{
		StringBuilder builder = new StringBuilder();
		for (String eb : destEbs) {
			String topicConnector = new String(client.getData().forPath(String.format("/topics/%s/%s", topicName, eb)));
			builder.append(String.format("%s;%s\n", eb, topicConnector));
		}
		return builder.toString();
	}

	public static void main(String args[]){
		if(args.length < 1){
			System.out.println("Usage:Test zkConnector");
			return;
		}
		String zkConnector= args[0];
		Test test= new Test(zkConnector);
		Thread testThread= new Thread(test);
		//callback to handle SIGINT and SIGTERM
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				test.stop();
				try{
					testThread.join();
				}catch(Exception e){
					e.getStackTrace();
				}
			}
		});
		testThread.start();
	}
}
