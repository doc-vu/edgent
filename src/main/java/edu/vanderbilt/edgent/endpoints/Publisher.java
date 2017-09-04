package edu.vanderbilt.edgent.endpoints;

import org.zeromq.ZMQ;

public class Publisher extends Container{
	public static final int PUBLISHER_PRODUCER_BASE_PORT_NUM=10000;
	private String producerConnector;
	private int pubId;
	private int sampleCount;
	private int sendInterval;
	private Producer producer;
	private Thread producerThread;

	public Publisher(String topicName,int id,
			int sampleCount,int sendInterval) {
		super(topicName, Container.ENDPOINT_TYPE_PUB, id);
		pubId=0;
		this.sampleCount=sampleCount;
		this.sendInterval=sendInterval;
		this.producerConnector=String.format("tcp://*:%d",
				(PUBLISHER_PRODUCER_BASE_PORT_NUM+pubId));
	}

	@Override
	public void initialize() {
		//start worker thread
		workers.put(pubId, new Sender(topicName,Worker.ENDPOINT_TYPE_PUB, pubId,
				commandConnector,queueConnector,producerConnector));
		workerThreads.put(pubId, new Thread(workers.get(pubId)));
		pubId++;
		workerThreads.get(0).start();
	
		while(workers.get(0).connected()==Worker.STATE_DISCONNECTED){
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
				return;
			}
		}
		if(workers.get(0).connected()==Worker.STATE_CONNECTED){
			// start the producer thread
			System.out.println("Started producer");
			producer=new Producer(context, topicName,  queueConnector,
					producerConnector, sampleCount,sendInterval);
			producerThread = new Thread(producer);
			producerThread.start();
		}
	}

	@Override
	public void cleanup() {
		try{
			producer.stop();
			producerThread.join();
		}catch(InterruptedException e){
			logger.error("Container:{} caught exception:{}",
					containerId,e.getMessage());
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
			
			//initialize subscriber
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
