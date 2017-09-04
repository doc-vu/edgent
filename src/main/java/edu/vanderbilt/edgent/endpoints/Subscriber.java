package edu.vanderbilt.edgent.endpoints;

import org.zeromq.ZMQ;

public class Subscriber extends Container{
	//Base port number at which the collector thread of Subscriber container listens for data
	public static final int SUBSCRIBER_COLLECTOR_BASE_PORT_NUM=7000;
	//Collector Thread
	private Thread collectorThread;
	//Connector for the collector thread
	private String collectorConnector;
	//expected number of samples
	private int sampleCount;
	private int subId;

	public Subscriber(String topicName,int id,int sampleCount){
		super(topicName,Container.ENDPOINT_TYPE_SUB,id);
		this.sampleCount=sampleCount;
		subId=0;
		collectorConnector=String.format("tcp://*:%d",
				(SUBSCRIBER_COLLECTOR_BASE_PORT_NUM+id));
	}

	@Override
	public void initialize() {
		//start the collector thread
		collectorThread = new Thread(new Collector(context, topicName, commandConnector, queueConnector,
				collectorConnector, sampleCount));
		collectorThread.start();
		logger.info("Container:{} started its data collector thread", containerId);

		workers.put(subId, new Receiver(topicName,Worker.ENDPOINT_TYPE_SUB, subId,
				commandConnector,queueConnector,collectorConnector));
		workerThreads.put(subId, new Thread(workers.get(subId)));
		subId++;
		workerThreads.get(0).start();
		logger.info("Container:{} started its default receiver thread", containerId);
	}

	@Override
	public void cleanup() {
		try{
			collectorThread.join();
		}catch(InterruptedException e){
			logger.error("Container:{} caught exception:{}",
					containerId,e.getMessage());
		}
	}

	public static void main(String args[]){
		if(args.length < 3){
			System.out.println("Subscriber topicName id sampleCount");
			return;
		}
		try{
			//parse commandline args
			String topicName = args[0];
			int id = Integer.parseInt(args[1]);
			int sampleCount=Integer.parseInt(args[2]);
			
			//initialize subscriber
			Subscriber sub=new Subscriber(topicName,id,sampleCount);
			Thread subThread = new Thread(sub);

			//install hook to handle SIGTERM and SIGINT
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					try {
						ZMQ.Context context= ZMQ.context(1);
						ZMQ.Socket pushSocket= context.socket(ZMQ.PUSH);
						pushSocket.connect(sub.queueConnector());
						pushSocket.send(Subscriber.CONTAINER_EXIT_COMMAND);
						subThread.join();
						pushSocket.setLinger(0);
						pushSocket.close();
						context.term();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			});
			//start subscriber
			subThread.start();
		}catch(NumberFormatException e){
			e.printStackTrace();
		}
	}
}
