package edu.vanderbilt.edgent.rebalancing.test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

public class Util {

	public static void createTopic(CuratorFramework client,String topicName, List<String> ebs) throws Exception{
		// create topic topicName on all destination Ebs
		for (String eb : ebs) {
			client.create().forPath(String.format("/eb/%s/%s", eb,topicName));
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

		System.out.format("Will wait until topic:%s is allocted on all dest ebs:%s\n",
				topicName, ebs.toString());
		latch.await();
		cache.close();
		System.out.format("Topic:%s was allocated on all dest ebs\n",
				topicName);
	}
}
