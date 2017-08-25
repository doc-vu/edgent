package edu.vanderbilt.edgent.clients;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import edu.vanderbilt.edgent.types.DataSampleHelper;
import edu.vanderbilt.edgent.util.UtilMethods;
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
	//This publisher's znode for the experiment run
	private String expZnodePath;
	//Publisher barrier to wait on until all test publishers have joined
	private DistributedBarrier pubBarrier;	

	
	public Publisher(String topicName, int runId,
			int sampleCount, int sendInterval,String zkConnector) throws Exception{
		//initialize Client endpoint by calling the super constructor
		super(Client.TYPE_PUB,topicName,sampleCount);

		//stash implementation specific state variables
		this.sendInterval=sendInterval;
		regionId=UtilMethods.regionId();
	
		//initialize connection to ZK
		client= CuratorFrameworkFactory.newClient(zkConnector,
				new ExponentialBackoffRetry(1000, 3));
		client.start();
		
		//publisher's znode for this experiment's run
		expZnodePath= String.format("/experiment/%s/pub/%s",
				runId,id);
		pubBarrier= new DistributedBarrier(client,
				String.format("/experiment/%s/barriers/pub", runId));
	}

	@Override
	public void process() {
		//setup experiment 
		waitForOtherPublishers();
		//only publish while we are connected to EB and sampleCount has not been reached
		while(connectionState.get()==Client.STATE_CONNECTED && currCount<sampleCount){
			//send data
			socket.sendMore(topicName.getBytes());
			socket.send(DataSampleHelper.serialize(currCount, // sample id
					regionId, runId, priority, System.currentTimeMillis(), payloadSize));
			currCount++;
			if(currCount%1000==0){
				logger.debug("{}:{} for topic:{} sent:{} samples",
						endpointType,id,topicName, currCount);
			}

			//sleep for average inter-arrival time
			long sleep_interval = exponentialInterarrival(sendInterval);
			if (sleep_interval > 0) {
				try {
					Thread.sleep(sleep_interval);
				} catch (InterruptedException e) {
					logger.error("{}:{} for topic:{} caught exception:{}",
						endpointType,id,topicName, e.getMessage());
				}
			}
		}
	}
	
	@Override
	public void onExit(){
		try {
			if(client.getState()==CuratorFrameworkState.STARTED){
				client.delete().forPath(expZnodePath);
			}
		} catch (Exception e) {
			logger.error("{}:{} for topic:{} caught exception:{}",endpointType,
				id,e.getMessage());
		}
		CloseableUtils.closeQuietly(client);
		logger.info("{}:{} for topic:{} deleted its experiment znode:{} and closed zk connection",
				endpointType,id,topicName,expZnodePath);
	}

	@Override
	public void onConnected() {
		try {
			if(client.checkExists().forPath(expZnodePath)==null){
				client.create().forPath(expZnodePath, new byte[0]);
				logger.info("{}:{} for topic:{} created its experiment znode:{}",endpointType,
						id, topicName, expZnodePath);
			}
		} catch (Exception e) {
			logger.error("{}:{} for topic:{} caught exception:{}",endpointType,
					id,topicName,e.getMessage());
		}
	}

	
	private void waitForOtherPublishers(){
		try{
			//wait until all test publishers have joined
			logger.info("{}:{} for topic:{} will wait on publisher barrier until all publishers have joined",
					endpointType,id, topicName);
			pubBarrier.waitOnBarrier();
			logger.info("{}:{} for topic:{} will start as all publishers have joined",
					endpointType, id, topicName);
		}catch(Exception e){
			logger.error("{}:{} for topic:{} caught exception:{}",endpointType,
					id,topicName,e.getMessage());
		}
	}

	/*
	 * Poisson inter-arrival time
	 */
	private long exponentialInterarrival(double averageInterval){
		return (long)(averageInterval*(-Math.log(Math.random())));
	}


	public static void main(String args[]){
		if (args.length < 5) {
			System.out.println(
					"Usage: Publisher topicName, runId, sampleCount, sendInterval, zkConnector");
			return;
		}
		try{
			//parse commandline arguments
			String topicName = args[0];
			int runId = Integer.parseInt(args[1]);
			int sampleCount = Integer.parseInt(args[2]);
			int sendInterval = Integer.parseInt(args[3]);
			String zkConnector= args[4];

			//initialize Publisher
			Publisher publisher = new Publisher(topicName, runId,
					sampleCount, sendInterval, zkConnector);

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
		}catch(Exception e){
			e.printStackTrace();
		}
	}

}
