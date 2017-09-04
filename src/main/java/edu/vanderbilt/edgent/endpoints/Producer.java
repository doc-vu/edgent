package edu.vanderbilt.edgent.endpoints;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import edu.vanderbilt.edgent.types.DataSampleHelper;
import edu.vanderbilt.edgent.util.UtilMethods;

public class Producer implements Runnable {
	private ZMQ.Context context;
	private ZMQ.Socket pubSocket;
	private ZMQ.Socket commandSocket;
	
	private String topicName;
	private String queueConnector;
	private String producerConnector;

	private AtomicBoolean stopped;
	private int currCount;
	private int sampleCount;
	private int sendInterval;
	
	private int regionId;
	//Fields to experiment with in the future. Currently, no-op.
	private int priority=1;
	private int payloadSize=10;//for 64-bytes data sample 
	private int runId=0;
	
	private Logger logger;

	public Producer(Context context, String topicName, String queueConnector,
			String producerConnector, int sampleCount, int sendInterval) {
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		this.context=context;
		this.topicName=topicName;
		this.queueConnector=queueConnector;
		this.producerConnector=producerConnector;
		this.sampleCount=sampleCount;
		this.sendInterval=sendInterval;
		
		currCount=0;
		stopped=new AtomicBoolean(false);
		regionId=UtilMethods.regionId();
		logger.debug("Producer initialized");
	}

	@Override
	public void run() {
		pubSocket= context.socket(ZMQ.PUB);
		pubSocket.bind(producerConnector);
		
		commandSocket=context.socket(ZMQ.PUSH);
		commandSocket.connect(queueConnector);
	
		//TODO: Adhoc sleep to prevent loss of initially sent data samples
		try{
			Thread.sleep(5000);
		}catch(InterruptedException e){
			logger.error("Producer thread:{} for topic:{} caught exception:{}",
					Thread.currentThread().getName(),topicName,e.getMessage());
		}
	
		while (!Thread.currentThread().isInterrupted() && 
				!stopped.get() && (currCount < sampleCount || sampleCount==-1)) {
			try {
				//send data 
				pubSocket.sendMore(topicName.getBytes());
				pubSocket.send(DataSampleHelper.serialize(currCount, // sample id
						regionId, runId, priority, System.currentTimeMillis(), payloadSize));
				if (currCount % 1000 == 0) {
					logger.debug("Producer for topic:{} sent:{} samples", topicName, currCount);
				}

				currCount++;

				// sleep for average inter-arrival time
				long sleep_interval = exponentialInterarrival(sendInterval);
				if (sleep_interval > 0) {
					Thread.sleep(sleep_interval);
				}
			} catch (InterruptedException e) {
				logger.error("Producer for topic:{} caught exception:{}", topicName, e.getMessage());
				break;
			}
		}
		//send exit signal to parent Publisher container if all messages have been sent
		if(currCount==sampleCount){
			commandSocket.send(Publisher.CONTAINER_EXIT_COMMAND);
		}
	
		//set linger to 0
		pubSocket.setLinger(0);
		commandSocket.setLinger(0);
	
		//close sockets
		pubSocket.close();
		commandSocket.close();
	}
	
	public void stop(){
		stopped.set(true);
	}
	
	private long exponentialInterarrival(double averageInterval){
		return (long)(averageInterval*(-Math.log(Math.random())));
	}
	
}
