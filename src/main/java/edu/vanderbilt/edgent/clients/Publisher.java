package edu.vanderbilt.edgent.clients;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import edu.vanderbilt.edgent.types.DataSampleHelper;
import edu.vanderbilt.edgent.util.UtilMethods;

/**
 * Client publisher end-point to test the system.
 * @author kharesp
 *
 */
public class Publisher {
	private String topicName;
	//identifier of the region to which this publisher belongs
	private int regionId;
	
	//experiment runId 
	private int runId;
	//count of samples to send and average inter-arrival time
	private int sampleCount;
	private int sendInterval;

	//ZMQ context and PUB socket
	private ZMQ.Context context;
	private ZMQ.Socket socket;

	//Fields to experiment with in the future. Currently, no-op.
	private int priority=1;
	private int payloadSize=10;//for 64-bytes data sample 
	
	//Curator client to connect to zk
	private CuratorFramework client;
	private String znodePath;
	//Publisher barrier to wait on until all test publishers have joined
	private DistributedBarrier pubBarrier;	

	private Logger logger;
	
	public Publisher(String topicName,int regionId,int runId,
			int sampleCount, int sendInterval,String zkConnector){
		logger= LogManager.getLogger(this.getClass().getSimpleName());

		this.topicName=topicName;
		this.regionId=regionId;

		this.sampleCount=sampleCount;
		this.sendInterval=sendInterval;
		
		context= ZMQ.context(1);
		socket= context.socket(ZMQ.PUB);
		
		client= CuratorFrameworkFactory.newClient(zkConnector,
				new ExponentialBackoffRetry(1000, 3));
		client.start();
		znodePath= String.format("/experiment/%s/pub/pub_%s_%s",
				runId,UtilMethods.hostName(),UtilMethods.pid());
		pubBarrier= new DistributedBarrier(client, String.format("/experiment/%s/barriers/pub", runId));

		logger.debug("Initialized publisher end-point for topic:{}",topicName);
	}
	
	public void start(String ebLocator) {
		try{
			//connect to EdgeBroker hosting the topic of interest
			socket.connect(ebLocator);
			logger.debug("Publisher for topic:{} connected to EB locator:{}", topicName, ebLocator);
			
			//Create znode for this publisher under /experiment/runId/pub
			client.create().forPath(znodePath,new byte[0]);
			logger.debug("Publisher for topic:{} created znode at:{}", topicName, znodePath);
	
			//Wait for all publishers to join before starting
			logger.debug("Publisher for topic:{} will wait on publisher barrier until all publishers have joined", topicName);
			pubBarrier.waitOnBarrier();
			logger.debug("Publisher for topic:{} will start as all publishers have joined.", topicName);

			//Once all publishers have joined, start publishing
			publish();
			logger.debug("Publisher for topic:{} sent all {} samples. Exiting",topicName,sampleCount);

			//Close zmq pub sockect before exiting
			socket.close();
			context.term();
			logger.debug("Publisher for topic:{} ZMQ PUB socket closed.", topicName);

			//Delete znode for this publisher under /experiment/runId/pub
			client.delete().forPath(znodePath);
			logger.debug("Publisher for topic:{} deleted znode at:{}.", topicName,znodePath);
		}catch(Exception e){
			logger.error(e.getMessage());

		}finally {
			CloseableUtils.closeQuietly(client);
		}
	}
	
	public void publish(){
		logger.debug("Publisher for topic:{} will wait for sometime before publishing.", topicName);
		try{
			Thread.sleep(5000);
		}catch(Exception e){
			e.printStackTrace();
		}
		logger.debug("Publisher for topic:{} will start publishing.", topicName);

		for(int i=0;i< sampleCount; i++){
			socket.sendMore(topicName.getBytes());
			socket.send(DataSampleHelper.serialize(i,//sample id
					regionId,
					runId,
					priority,
					System.currentTimeMillis(),
					payloadSize));
			try {
				long sleep_interval = exponentialInterarrival(sendInterval);
				if (sleep_interval > 0) {
					Thread.sleep(sleep_interval);
				}
			} catch (InterruptedException ix) {
				break;
			}
			
			if(i%1000==0){
				logger.debug("Publisher for topic:{} sent:{} samples",topicName,i);
			}
		}
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
			// TODO acquire the address of EB which hosts the topic of interest
			int port=TemporaryHelper.topicSendPort(topicName);
			publisher.start(String.format("tcp://10.2.2.47:%d",port));
			
		}catch(NumberFormatException e){
			e.printStackTrace();
		}
	}

	/**
	 * Helper method to model poisson arrival 
	 * @param averageInterval
	 * @return sleep interval
	 */
	private long exponentialInterarrival(double averageInterval){
		return (long)(averageInterval*(-Math.log(Math.random())));
	}

}
