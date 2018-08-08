package edu.vanderbilt.edgent.endpoints.publisher;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import edu.vanderbilt.edgent.types.DataSampleHelper;
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
	//signal to track if producer is initialized or not
	private CountDownLatch producerInitialized;

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
	private int payloadSize;
	private int runId=0;

	private DataSampleHelper fb;
	private CuratorFramework client;
	private String experimentType;
	
	private boolean send;
	private Logger logger;

	public Producer(String containerId,Context context, String topicName, String queueConnector,CountDownLatch producerInitialized,
			 int sampleCount, int sendInterval,int payloadSize,
			String zkConnector,String experimentType,boolean send) {
		logger= LogManager.getLogger(this.getClass().getName());
		//stash constructor arguments
		this.containerId=containerId;
		this.context=context;
		this.topicName=topicName;
		this.queueConnector=queueConnector;
		this.producerInitialized=producerInitialized;
		this.sampleCount=sampleCount;
		this.sendInterval=sendInterval;
		this.payloadSize=payloadSize;
		this.experimentType=experimentType;
		this.fb=new DataSampleHelper();
		this.send=send;
		
		currCount=0;
		regionId=UtilMethods.regionId();
		stopped=new AtomicBoolean(false);
		
		//initialize curator client for ZK connection
		client=CuratorFrameworkFactory.newClient(zkConnector,300000,300000,
				new ExponentialBackoffRetry(1000, 100));
		client.start();
		
		logger.debug("Producer:{} initialized",containerId);
	}

	@Override
	public void run() {
		try{
			commandSocket = context.socket(ZMQ.PUSH);
			commandSocket.connect(queueConnector);

			pubSocket = context.socket(ZMQ.PUB);
			pubSocket.setHWM(0);
			int port=pubSocket.bindToRandomPort("tcp://*");
			producerConnector=String.format("tcp://*:%d",port);
			producerInitialized.countDown();

			boolean barrier_opened=false;
			DistributedBarrier barrier = new DistributedBarrier(client,
					String.format("/experiment/%s/barriers/pub",experimentType));
			while(!stopped.get() && !barrier_opened){
				logger.info("Producer:{} will wait until all publishers have joined",containerId);
				barrier_opened=barrier.waitOnBarrier(5, TimeUnit.SECONDS);
			}
			logger.info("Producer:{} wait on barrier was successful.Will start sending data",containerId);

			// TODO: Adhoc sleep to prevent loss of initially sent data samples
			Thread.sleep(1000);

			while (!Thread.currentThread().isInterrupted() && !stopped.get()
					&& (currCount < sampleCount || sampleCount == -1)) {
				try {
					// send data
					if(this.send && barrier_opened){
						pubSocket.sendMore(topicName.getBytes());
						pubSocket.send(fb.serialize(currCount, // sample
																			// id
							regionId, runId, priority, System.currentTimeMillis(), // pubSendTs
							-1, // ebReceiveTs
							containerId, payloadSize));
						if (currCount % 10 == 0) {
							logger.info("Producer:{} for topic:{} sent:{} samples", containerId, topicName, currCount);
						}

						currCount++;
					}

					// sleep for average inter-arrival time
					long sleep_interval = exponentialInterarrival(sendInterval);
					if (sleep_interval > 0) {
						Thread.sleep(sleep_interval);
					}
				} catch (InterruptedException e) {
					logger.error("Producer:{} for topic:{} caught exception:{}", containerId, topicName,
							e.getMessage());
					break;
				}
			}
			// send exit signal to parent container if all messages have been sent
			if (currCount == sampleCount) {
				logger.info("Producer:{} for topic:{} sent all {} messages", containerId, topicName, sampleCount);
				// commandSocket.send(ContainerCommandHelper.serialize(Commands.CONTAINER_EXIT_COMMAND));
				client.create().forPath(String.format("/experiment/%s/sent/%s",experimentType, containerId));
			}
			clean();

			logger.info("Producer:{} for topic:{} has exited", containerId, topicName);
		} catch (Exception e) {
			logger.error("Producer:{} for topic:{} caught exception:{}", containerId, topicName,
					e.getMessage());
			clean();
		}
	}
	
	private void clean(){
		// set linger to 0
		pubSocket.setLinger(0);
		commandSocket.setLinger(0);

		// close sockets
		pubSocket.close();
		commandSocket.close();

		CloseableUtils.closeQuietly(client);
	}
	
	public void stop(){
		stopped.set(true);
	}
	
	public String producerConnector(){
		return producerConnector;
	}
	
	private long exponentialInterarrival(double averageInterval){
		return (long)(averageInterval*(-Math.log(Math.random())));
	}
	
}
