package edu.vanderbilt.edgent.endpoints;

import org.zeromq.ZMQ;

import edu.vanderbilt.edgent.types.ContainerCommandHelper;
import edu.vanderbilt.edgent.util.Commands;


public class Publisher extends Container{
	//Base port number for the producer thread at which data is sent out
	public static final int PUBLISHER_PRODUCER_BASE_PORT_NUM=10000;
	//Producer instance
	private Producer producer;
	//Thread running the data producer
	private Thread producerThread;
	//Connector at which producer thread publishes data
	private String producerConnector;

	//number of samples to send
	private int sampleCount;
	//sending rate of publisher
	private int sendInterval;

	public Publisher(String topicName,int id,
			int sampleCount,int sendInterval) {
		super(topicName, Container.ENDPOINT_TYPE_PUB, id);
		this.sampleCount=sampleCount;
		this.sendInterval=sendInterval;
		producerConnector=String.format("tcp://*:%d",
				(PUBLISHER_PRODUCER_BASE_PORT_NUM+id));
	}

	@Override
	public void initialize() {
		//no-op
	}

	@Override
	public void onConnected(){
		System.out.println("ON CONNECTED CALLED!!!!");
		// start the producer thread
		producer = new Producer(context, topicName, queueConnector, producerConnector, sampleCount, sendInterval);
		producerThread = new Thread(producer);
		producerThread.start();
		logger.info("Container:{} started its data producer thread", containerId);
	}

	@Override
	public void cleanup() {
		if (producer != null) {
			try {
				producer.stop();
				producerThread.join();
			} catch (InterruptedException e) {
				logger.error("Container:{} caught exception:{}",
						containerId, e.getMessage());
			}
		}
	}
	
	public static void main(String args[]){
		if(args.length < 4){
			System.out.println("Publisher topicName id sampleCount sendInterval");
			return;
		}
		try{
			//parse commandline args
			String topicName = args[0];
			int id = Integer.parseInt(args[1]);
			int sampleCount=Integer.parseInt(args[2]);
			int sendInterval=Integer.parseInt(args[3]);
			
			//initialize publisher 
			Publisher pub=new Publisher(topicName,id,sampleCount,sendInterval);
			Thread pubThread = new Thread(pub);

			//install hook to handle SIGTERM and SIGINT
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					try {
						ZMQ.Context context= ZMQ.context(1);
						ZMQ.Socket pushSocket= context.socket(ZMQ.PUSH);
						pushSocket.connect(pub.queueConnector());
						pushSocket.send(ContainerCommandHelper.serialize(Commands.CONTAINER_EXIT_COMMAND));
						pubThread.join();
						pushSocket.setLinger(0);
						pushSocket.close();
						context.term();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			});
			//start publisher
			pubThread.start();
		}catch(NumberFormatException e){
			e.printStackTrace();
		}
	}

	@Override
	public Worker createWorker(int uuid,String ebId, String topicConnector) {
		return new Sender(containerId, uuid, 
				topicName, Worker.ENDPOINT_TYPE_PUB,ebId, 
				topicConnector,
				commandConnector, queueConnector,producerConnector);
	}

}
