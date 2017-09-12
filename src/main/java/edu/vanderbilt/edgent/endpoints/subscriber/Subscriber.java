package edu.vanderbilt.edgent.endpoints.subscriber;

import org.zeromq.ZMQ;
import edu.vanderbilt.edgent.endpoints.Container;
import edu.vanderbilt.edgent.endpoints.Worker;
import edu.vanderbilt.edgent.types.ContainerCommandHelper;
import edu.vanderbilt.edgent.util.Commands;
import edu.vanderbilt.edgent.util.PortList;

public class Subscriber extends Container{
	//Collector Thread
	private Thread collectorThread;
	//Connector for the collector thread
	private String collectorConnector;
	//expected number of samples
	private int sampleCount;

	public Subscriber(String topicName,int id,int sampleCount){
		super( topicName,Container.ENDPOINT_TYPE_SUB,id);
		this.sampleCount=sampleCount;
		collectorConnector=String.format("tcp://*:%d",
				(PortList.SUBSCRIBER_COLLECTOR_BASE_PORT_NUM+id));
	}

	@Override
	public void initialize() {
		//start the collector thread
		collectorThread = new Thread(new Collector(context, topicName, commandConnector, queueConnector,
				collectorConnector, sampleCount));
		collectorThread.start();
		logger.info("Container:{} started its data collector thread", containerId);
	}

	@Override
	public void onConnected(){
		//no-op
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

	@Override
	public Worker instantiateWorker(int uuid, String ebId, String topicConnector) {
		return new Receiver(containerId, uuid,
				topicName, Worker.ENDPOINT_TYPE_SUB,ebId,
				topicConnector,
				commandConnector, queueConnector, collectorConnector);
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
						//send CONTAINER_EXIT_COMMAND
						pushSocket.send(ContainerCommandHelper.serialize(Commands.CONTAINER_EXIT_COMMAND));
						//wait for subscriber to exit
						subThread.join();
						//cleanup ZMQ
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
