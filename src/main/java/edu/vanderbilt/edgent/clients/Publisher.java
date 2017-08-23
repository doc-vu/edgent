package edu.vanderbilt.edgent.clients;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import edu.vanderbilt.edgent.types.DataSampleHelper;
/**
 * Client publisher end-point to test the system.
 * @author kharesp
 *
 */
public class Publisher extends Client {
	//identifier of the region to which this publisher belongs
	private int regionId;
	//experiment runId 
	private int runId;
	//average inter-arrival time
	private int sendInterval;
	//Fields to experiment with in the future. Currently, no-op.
	private int priority=1;
	private int payloadSize=10;//for 64-bytes data sample 


	//Curator client to connect to zk
	private CuratorFramework client;
	private String expZnodePath;
	//Publisher barrier to wait on until all test publishers have joined
	private DistributedBarrier pubBarrier;	


	
	public Publisher(String topicName,int regionId,int runId,
			int sampleCount, int sendInterval,String zkConnector){
		super(Client.TYPE_PUB,topicName,sampleCount);

		this.regionId=regionId;
		this.sendInterval=sendInterval;
	
		//initialize connection to ZK
		client= CuratorFrameworkFactory.newClient(zkConnector,
				new ExponentialBackoffRetry(1000, 3));
		client.start();
		expZnodePath= String.format("/experiment/%s/pub/%s",
				runId,id);
		pubBarrier= new DistributedBarrier(client,
				String.format("/experiment/%s/barriers/pub", runId));

	}

	
	
	
	@Override
	public void shutdown(){
		CloseableUtils.closeQuietly(client);
	}


	@Override
	public void process() {
		experimentSetup();

		//only publish while we are connected to EB and sampleCount has not been reached
		while(connectionState.get()==Client.STATE_CONNECTED && currCount<sampleCount){
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			//send data
			socket.sendMore(topicName.getBytes());
			socket.send(DataSampleHelper.serialize(currCount, // sample id
					regionId, runId, priority, System.currentTimeMillis(), payloadSize));
			currCount++;
			//if(currCount%1000==0){
				logger.debug("Publisher:{} for topic:{} sent:{} samples",id,topicName, currCount);
			//}

			//sleep for average inter arrival time
			long sleep_interval = exponentialInterarrival(sendInterval);
			if (sleep_interval > 0) {
				try {
					Thread.sleep(sleep_interval);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	private void experimentSetup(){
		try{
			if(client.checkExists().forPath(expZnodePath)==null){
				client.create().forPath(expZnodePath, new byte[0]);
				logger.debug("Publisher:{} for topic:{} created znode:{}", id, topicName, expZnodePath);
			}
			logger.debug("Publisher:{} for topic:{} will wait on publisher barrier until all publishers have joined",
					id, topicName);
			pubBarrier.waitOnBarrier();
			logger.debug("Publisher:{} for topic:{} will start as all publishers have joined", id, topicName);
		}catch(Exception e){
			logger.error("Publisher:{} for topic:{} caught exception:{}",id,topicName,e.getMessage());
		}
	}

	private long exponentialInterarrival(double averageInterval){
		return (long)(averageInterval*(-Math.log(Math.random())));
	}

	public static void main(String args[]){
		if (args.length < 6) {
			System.out.println(
					"Usage: Publisher topicName, regionId, runId, sampleCount, sendInterval, zkConnector");
			return;
		}

		try{
			String topicName = args[0];
			int regionId = Integer.parseInt(args[1]);
			int runId = Integer.parseInt(args[2]);
			int sampleCount = Integer.parseInt(args[3]);
			int sendInterval = Integer.parseInt(args[4]);
			String zkConnector= args[5];

			Publisher publisher = new Publisher(topicName, regionId, runId, sampleCount, sendInterval, zkConnector);
			//Handle SIGINT and SIGTERM
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					//stop publisher
					publisher.stop();
				}
			});
			//start publisher
			publisher.start();
		}catch(NumberFormatException e){
			e.printStackTrace();
		}
	}
}
