package edu.vanderbilt.edgent.endpoints.publisher;

import org.zeromq.ZMQ;
import edu.vanderbilt.edgent.endpoints.Container;
import edu.vanderbilt.edgent.endpoints.Worker;
import edu.vanderbilt.edgent.types.ContainerCommandHelper;
import edu.vanderbilt.edgent.util.Commands;
import edu.vanderbilt.edgent.util.PortList;

public class Publisher extends Container{
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
				(PortList.PUBLISHER_PRODUCER_BASE_PORT_NUM+id));
	}

	@Override
	public void initialize() {
		//no-op
	}

	@Override
	public void onConnected(){
		// start the producer thread
		producer = new Producer(containerId,context, topicName,
				queueConnector, producerConnector, sampleCount, sendInterval);
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

	@Override
	public Worker instantiateWorker(int uuid,String ebId, String topicConnector) {
		return new Sender(containerId, uuid, 
				topicName, Worker.ENDPOINT_TYPE_PUB,ebId, 
				topicConnector,
				commandConnector, queueConnector,producerConnector);
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
						//send CONTAINER_EXIT_COMMAND
						pushSocket.connect(pub.queueConnector());
						pushSocket.send(ContainerCommandHelper.serialize(Commands.CONTAINER_EXIT_COMMAND));
						//wait for publisher thread to exit
						pubThread.join();
						//cleanup ZMQ
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
