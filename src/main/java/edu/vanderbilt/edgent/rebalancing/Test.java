package edu.vanderbilt.edgent.rebalancing;

import java.util.concurrent.CountDownLatch;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class Test {
	private CuratorFramework client;

	public Test(String zkConnector){
		client= CuratorFrameworkFactory.newClient(zkConnector,
				new ExponentialBackoffRetry(1000, 3));
		client.start();
	}
	
	public void start(){
		//Install PathChildren
		PathChildrenCache cache = new PathChildrenCache(client, "/lbtest", true);
		cache.getListenable().addListener(new PathChildrenCacheListener(){

			@Override
			public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
				if(event.getType()==Type.CHILD_ADDED){
					String znodePath= event.getData().getPath();
					System.out.println(znodePath);
					String[] parts= znodePath.split("/");
					if(parts[2].equals("migration")){
						migration();
					}
					else if(parts[2].equals("allSub")){
						allSub();
					}
					else if(parts[2].equals("allPub")){
						allPub();
					}else{
						System.out.println("LB test command not recognized");
					}
				}
			}
			
		});
		try {
			cache.start();
			while(true){
				Thread.sleep(5000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	private void migration(){
		System.out.println("will test topic migration");
		try{
			//create /eb2/t1 znode 
			client.create().forPath("/eb/EB-0-127.0.0.1-2/t1");
			System.out.println("created /eb/EB-0-127.0.0.1-2/t1");

			//create /lb/topics/t1/eb2 znode
			client.create().forPath("/lb/topics/t1/EB-0-127.0.0.1-2");
			System.out.println("created /lb/topics/t1/EB-0-127.0.0.1-2");

			//wait for t1 to get allocated on eb2
			CountDownLatch latch = new CountDownLatch(1);

			PathChildrenCache cache = new PathChildrenCache(client, String.format("/topics/t1"), false);
			cache.getListenable().addListener(new PathChildrenCacheListener() {
				@Override
				public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
					if (event.getType().equals(Type.CHILD_ADDED)) {
						String znodePath=event.getData().getPath();
						String[] parts= znodePath.split("/");
						if(parts[3].equals("EB-0-127.0.0.1-2")){
							latch.countDown();
						}
					}
				}
			});
			cache.start();
			//block until notified
			System.out.println("will wait until t1 is allocated on EB-0-127.0.0.1-2");
			latch.await();
			System.out.println("t1 was allocated on EB-0-127.0.0.1-2");
			//t1 is also hosted on eb2
			
			//get the locator for topic t1 on eb2
			String topicLocator= new String(client.getData().forPath("/topics/t1/EB-0-127.0.0.1-2"));
			String data= String.format("EB-0-127.0.0.1-2;%s", topicLocator);
			
			//create /lb/topics/t1/EB-0-127.0.0.1-1/migration with t1's topic locator on EB2
			client.create().forPath("/lb/topics/t1/EB-0-127.0.0.1-1/migration",data.getBytes());

		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private void allSub(){
		System.out.println("will test all subscriber topic replication");
	}
	
	private void allPub(){
		System.out.println("will test all publisher topic replication");
	}

	public static void main(String args[]){
		if(args.length < 1){
			System.out.println("Usage:Test zkConnector");
			return;
		}
		String zkConnector= args[0];
		new Test(zkConnector).start();
	}
}
