package edu.vanderbilt.edgent.endpoints.publisher;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import edu.vanderbilt.edgent.types.ContainerCommandHelper;
import edu.vanderbilt.edgent.types.DataSampleHelper;
import edu.vanderbilt.edgent.util.Commands;
import edu.vanderbilt.edgent.util.UtilMethods;

public class Producer implements Runnable {
	//ZMQ context
	private ZMQ.Context context;
	//ZMQ.PUB socket at which produced data is published 
	private ZMQ.Socket pubSocket;
	//ZMQ.PUSH socket at which commands are sent to parent container's queue
	private ZMQ.Socket commandSocket;
	
	//parent container's queue connector 
	private String queueConnector;
	//connector at which produced data is published
	private String producerConnector;

	//Parent container's id
	private String containerId;

	private String topicName;
	//current count of samples produced/sent
	private int currCount;
	//total number of samples to be produced/sent
	private int sampleCount;
	//sending rate
	private int sendInterval;
	//flag to indicate whether to stop producing data
	private AtomicBoolean stopped;
	
	private int regionId;
	//Fields to experiment with in the future. Currently, no-op.
	private int priority=1;
	private int payloadSize=10;//for 64-bytes data sample 
	private int runId=0;
	
	private Logger logger;

	public Producer(String containerId,Context context, String topicName, String queueConnector,
			String producerConnector, int sampleCount, int sendInterval) {
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		//stash constructor arguments
		this.containerId=containerId;
		this.context=context;
		this.topicName=topicName;
		this.queueConnector=queueConnector;
		this.producerConnector=producerConnector;
		this.sampleCount=sampleCount;
		this.sendInterval=sendInterval;
		
		currCount=0;
		regionId=UtilMethods.regionId();
		stopped=new AtomicBoolean(false);
		logger.debug("Producer:{} initialized",containerId);
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
			logger.error("Producer:{} for topic:{} caught exception:{}",
					containerId,topicName,e.getMessage());
		}
	
		while (!Thread.currentThread().isInterrupted() && 
				!stopped.get() && (currCount < sampleCount || sampleCount==-1)) {
			try {
				//send data 
				pubSocket.sendMore(topicName.getBytes());
				pubSocket.send(DataSampleHelper.serialize(currCount, // sample id
						regionId, runId, priority, System.currentTimeMillis(), payloadSize));
				if (currCount % 1000 == 0) {
					logger.debug("Producer:{} for topic:{} sent:{} samples",containerId, topicName, currCount);
				}

				currCount++;

				// sleep for average inter-arrival time
				long sleep_interval = exponentialInterarrival(sendInterval);
				if (sleep_interval > 0) {
					Thread.sleep(sleep_interval);
				}
			} catch (InterruptedException e) {
				logger.error("Producer:{} for topic:{} caught exception:{}",containerId, topicName, e.getMessage());
				break;
			}
		}
		//send exit signal to parent container if all messages have been sent
		if(currCount==sampleCount){
			commandSocket.send(ContainerCommandHelper.serialize(Commands.CONTAINER_EXIT_COMMAND));
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
