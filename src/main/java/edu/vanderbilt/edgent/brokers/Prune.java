package edu.vanderbilt.edgent.brokers;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.curator.framework.CuratorFramework;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.data.Stat;
import edu.vanderbilt.edgent.util.Commands;

public class Prune implements Runnable {
	//Time interval after which an unused topic is removed
	private static final int TOPIC_EXPIRY_PERIOD_SEC=300;
	//EB's id 
	private String ebId;
	//Curator client for connection to ZK
	private CuratorFramework client;
	//EB's shared processing queue 
	private LinkedBlockingQueue<String> queue;

	private Logger logger;
	public Prune(String ebId, CuratorFramework client, LinkedBlockingQueue<String> queue){
		this.ebId=ebId;
		this.client=client;
		this.queue=queue;
		logger= LogManager.getLogger(this.getClass().getName());
	}

	@Override
	public void run() {
		try {
			logger.info("EdgeBroker:{} Periodic pruning scheduled on thread:{}",
					ebId,Thread.currentThread().getName());

			//Acquire list of hosted topics at this broker at: /eb/ebId
			List<String> topics=client.getChildren().forPath(String.format("/eb/%s",ebId));

			//For each topic, check if any interested endpoint exists
			for(String topicName: topics){
				logger.debug("Periodic pruning thread:{}: assessing topic:{}",
						Thread.currentThread().getName(),topicName);

					//get list of publishers for topicName under: /eb/ebId/topicName/pub
					List<String> publishers= client.getChildren().
							forPath(String.format("/eb/%s/%s/pub",ebId,topicName));

					if (publishers.isEmpty()) {
						//get list of subscribers for topicName under: /eb/ebId/topicName/sub
						List<String> subscribers = client.getChildren()
								.forPath(String.format("/eb/%s/%s/sub", ebId, topicName));

						if (subscribers.isEmpty()) {
							/*Both publishers and subscribers for this topic don't exist
							delete topic if it is older than set-timeout */
							Stat topicStat=client.checkExists().
									forPath(String.format("/topics/%s",topicName));
							long elapsed_milisec=System.currentTimeMillis()-topicStat.getCtime();

							logger.debug("EdgeBroker:{} Periodic pruning thread:{} topic:{}'s elapsed time:{}",
									ebId,Thread.currentThread().getName(),topicName,elapsed_milisec/1000);

							if(elapsed_milisec/1000 > TOPIC_EXPIRY_PERIOD_SEC){
								logger.info("EdgeBroker:{} Periodic pruning thread:{} will delete topic:{}",
									ebId,Thread.currentThread().getName(),topicName);
								queue.add(String.format("%s,%s", Commands.EB_TOPIC_DELETE_COMMAND,topicName));
							}
						}
					}
			}

		} catch (Exception e) {
			logger.error("EdgeBroker:{} Periodic pruning thread:{} caught exception:{}",ebId,
					Thread.currentThread().getName(),
					e.getMessage());
		}
	}
}
