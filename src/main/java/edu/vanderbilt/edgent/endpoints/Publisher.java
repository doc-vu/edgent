package edu.vanderbilt.edgent.endpoints;

import org.zeromq.ZMQ;

public class Publisher extends Container{
	//Base port number for the producer thread at which data is sent out
	public static final int PUBLISHER_PRODUCER_BASE_PORT_NUM=10000;
	//Producer instance
	private Producer producer;
	//Thread running the data producer
	private Thread producerThread;
	//Connector at which producer thread publishes data
	private String producerConnector;

	private int pubId;
	//number of samples to send
	private int sampleCount;
	//sending rate of publisher
	private int sendInterval;

	public Publisher(String topicName,int id,
			int sampleCount,int sendInterval) {
		super(topicName, Container.ENDPOINT_TYPE_PUB, id);
		this.sampleCount=sampleCount;
		this.sendInterval=sendInterval;
		pubId=0;
		producerConnector=String.format("tcp://*:%d",
				(PUBLISHER_PRODUCER_BASE_PORT_NUM+pubId));
	}

	@Override
	public void initialize() {
		//create default sender thread with pubId=0
		workers.put(pubId, new Sender(topicName,Worker.ENDPOINT_TYPE_PUB, pubId,
				commandConnector,queueConnector,producerConnector));
		workerThreads.put(pubId, new Thread(workers.get(pubId)));
		pubId++;
		//start default sender thread
		workerThreads.get(0).start();
		logger.info("Container:{} started its default sender thread", containerId);

		//wait until default sender thread is in the connected state, before starting the producer
		while(workers.get(0).connected()==Worker.STATE_DISCONNECTED){
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				logger.error("Container:{} caught exception:{}",
						containerId,e.getMessage());
			}
		}
		if(workers.get(0).connected()==Worker.STATE_CONNECTED){
			// start the producer thread
			producer=new Producer(context, topicName,  queueConnector,
					producerConnector, sampleCount,sendInterval);
			producerThread = new Thread(producer);
			producerThread.start();
			logger.info("Container:{} started its data producer thread", containerId);
		}
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
						pushSocket.send(Subscriber.CONTAINER_EXIT_COMMAND);
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

}
