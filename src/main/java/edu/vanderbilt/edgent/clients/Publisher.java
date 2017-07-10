package edu.vanderbilt.edgent.clients;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import edu.vanderbilt.edgent.types.DataSampleHelper;

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
	
	private ZMQ.Context context;
	private ZMQ.Socket socket;

	//Fields to experiment with in the future. Currently, no-op.
	private int priority=1;
	private int payloadSize=10;//for 64-bytes data sample 
	
	private Logger logger;
	
	public Publisher(String topicName,int regionId,int runId,
			int sampleCount, int sendInterval){
		logger= LogManager.getLogger(this.getClass().getSimpleName());

		this.topicName=topicName;
		this.regionId=regionId;

		this.sampleCount=sampleCount;
		this.sendInterval=sendInterval;

		context= ZMQ.context(1);
		socket= context.socket(ZMQ.PUB);

		logger.debug("Initialized publisher end-point for topic:{}",topicName);
	}
	
	public void publish(String ebLocator) {
		socket.connect(ebLocator);

		try{
			Thread.sleep(5000);
		}catch(Exception e){
			e.printStackTrace();
		}
		
		logger.debug("Publisher for topic {} connected to EB:{}. Will start publishing",
				topicName,
				ebLocator);

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
			
			//if(i%1000==0){
				logger.debug("Publisher for topic:{} sent:{} samples",topicName,i);
			//}
		}
		logger.debug("Publisher for topic:{} sent all {} samples. Exiting",topicName,sampleCount);

		socket.close();
		context.term();
	}
	
	public static void main(String args[]){
		if (args.length < 5) {
			System.out.println(
					"Usage: Publisher topicName, regionId, runId, sampleCount, sendInterval");
			return;
		}

		try{
			String topicName = args[0];
			int regionId = Integer.parseInt(args[1]);
			int runId = Integer.parseInt(args[2]);
			int sampleCount = Integer.parseInt(args[3]);
			int sendInterval = Integer.parseInt(args[4]);

			Publisher publisher = new Publisher(topicName, regionId, runId, sampleCount, sendInterval);
			// TODO acquire the address of EB which hosts the topic of interest
			publisher.publish("tcp://10.2.2.47:5000");
			
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
