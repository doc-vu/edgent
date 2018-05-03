package edu.vanderbilt.edgent.endpoints.subscriber;

import org.zeromq.ZMQ;
import edu.vanderbilt.edgent.endpoints.Container;
import edu.vanderbilt.edgent.endpoints.Worker;
import edu.vanderbilt.edgent.types.ContainerCommandHelper;
import edu.vanderbilt.edgent.util.Commands;
import edu.vanderbilt.edgent.util.PortList;

public class Subscriber extends Container{
	//Collector Thread
	private Thread collectorThread=null;
	//Connector for the collector thread
	private String collectorConnector;
	//expected number of samples
	private int sampleCount;
	//experiment runId
	private int runId;
	//log directory 
	private String logDir;
	
	private boolean logLatency;

	public Subscriber(String topicName, int id, int sampleCount,
			int runId, String logDir,boolean logLatency,String experimentType,int interval,String feAddress){
		super( topicName,Container.ENDPOINT_TYPE_SUB,id,experimentType,interval,feAddress);
		this.sampleCount=sampleCount;
		this.runId=runId;
		this.logDir=logDir;
		this.logLatency=logLatency;
		collectorConnector=String.format("tcp://localhost:%d",
				(PortList.SUBSCRIBER_COLLECTOR_BASE_PORT_NUM+id));
	}

	@Override
	public void initialize() {
		//start the collector thread
		collectorThread = new Thread(new Collector(containerId,context, topicName, commandConnector, queueConnector,
				collectorConnector, sampleCount,runId, logDir,logLatency));
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
			if(collectorThread!=null){
				collectorThread.join();
			}
		}catch(InterruptedException e){
			logger.error("Container:{} caught exception:{}",
					containerId,e.getMessage());
		}
	}

	@Override
	public Worker instantiateWorker(ZMQ.Context context,int uuid, String ebId, String topicConnector) {
		return new Receiver(context,containerId, uuid,
				topicName, Worker.ENDPOINT_TYPE_SUB,ebId,
				topicConnector,
				commandConnector, queueConnector, collectorConnector);
	}

	public static void main(String args[]){
		if(args.length < 9){
			System.out.println("Subscriber topicName id sampleCount runId logDir logLatency experimentType interval feAddress");
			return;
		}
		try{
			//parse commandline args
			String topicName = args[0];
			int id = Integer.parseInt(args[1]);
			int sampleCount=Integer.parseInt(args[2]);
			int runId=Integer.parseInt(args[3]);
			String logDir= args[4];
			boolean logLatency= Integer.parseInt(args[5])>0?true:false;
			String experimentType=args[6];
			int interval= Integer.parseInt(args[7]);
			String feAddress=args[8];
			
			//initialize subscriber
			Subscriber sub=new Subscriber(topicName,id,sampleCount,runId,logDir,logLatency,experimentType,interval,feAddress);
			Thread subThread = new Thread(sub);

			//install hook to handle SIGTERM and SIGINT
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					try {
						ContainerCommandHelper containerCommandHelper=new ContainerCommandHelper();
						ZMQ.Context context= ZMQ.context(1);
						ZMQ.Socket pushSocket= context.socket(ZMQ.PUSH);
						pushSocket.connect(sub.queueConnector());
						//send CONTAINER_EXIT_COMMAND
						pushSocket.send(containerCommandHelper.serialize(Commands.CONTAINER_EXIT_COMMAND));
						//wait for subscriber to exit
						subThread.join();
						//cleanup ZMQ
						pushSocket.setLinger(0);
						pushSocket.close();
						context.close();
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
