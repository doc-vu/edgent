package edu.vanderbilt.edgent.subscriber;

import java.util.HashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import edu.vanderbilt.edgent.util.UtilMethods;
/**
 * Subscriber container comprises of the following components: 
 * 1. Atleast one Receiver thread which receives data for the topic of interest. 
 * During reconfiguration, there may be more than one Receiver threads
 * until reconfiguration completes. 
 * 2. Collector thread which collects received messages from the Receiver threads
 * and performs de-duplication of messages. 
 * 3. commandQueue, in which LB commands are enqueued by the Receiver's LB listener thread. 
 * The Subscriber container processes the commands placed in the commandQueue. 
 * @author kharesp
 */
public class Subscriber implements Runnable{
	//Type of commands processed by the Subscriber
	public static final String SUBSCRIBER_EXIT_COMMAND="exit";
	//base port numbers used by Subscriber
	public static final int SUBSCRIBER_COMMAND_BASE_PORT_NUM=16100;
	public static final int SUBSCRIBER_COLLECTOR_BASE_PORT_NUM=9100;
	public static final int SUBSCRIBER_QUEUE_BASE_PORT_NUM=4100; 

	//ZMQ context
	private ZMQ.Context context;
	//ZMQ.PUB socket to send control messages to Receiver and Collector threads
	private ZMQ.Socket commandSocket;
	//ZMQ.PULL socket to receive commands for the subscriber container
	private ZMQ.Socket subQueueSocket;

	//Connector for the commandSocket
	private String commandConnector;
	//Connector for the collector thread
	private String collectorConnector;
	//Connector for the queue
	private String subQueueConnector;
	
	//Collector Thread
	private Thread collectorThread;

	private String topicName;
	private int receiverId;
	private int sampleCount;

	//Map to hold references to Receivers
	private HashMap<Integer,Receiver> receivers;
	//Map to hold references to Receiver threads
	private HashMap<Integer,Thread> receiverThreads;

	private String subId;
	private Logger logger;

	public Subscriber(String topicName,int id,int sampleCount){
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		this.topicName=topicName;
		this.sampleCount=sampleCount;
		subId=String.format("%s-%s-%d",UtilMethods.hostName(),topicName,id);

		receiverId=0;
		receivers=new HashMap<Integer,Receiver>();
		receiverThreads=new HashMap<Integer,Thread>();

		this.context=ZMQ.context(1);
		commandConnector=String.format("tcp://*:%d",
				(SUBSCRIBER_COMMAND_BASE_PORT_NUM+id));
		collectorConnector=String.format("tcp://*:%d",
				(SUBSCRIBER_COLLECTOR_BASE_PORT_NUM+id));
		subQueueConnector=String.format("tcp://*:%d",
				(SUBSCRIBER_QUEUE_BASE_PORT_NUM+id));

		logger.debug("Subscriber:{} initialized",subId);
	}

	@Override
	public void run() {
		logger.info("Subscriber:{} started", subId);

		//create and bind subscriber's queue socket 
		subQueueSocket= context.socket(ZMQ.PULL);
		subQueueSocket.bind(subQueueConnector);

		//create and bind subscriber's command socket
		commandSocket= context.socket(ZMQ.PUB);
		commandSocket.bind(commandConnector);

		//start the collector thread
		collectorThread= new Thread(new Collector(context,topicName,commandConnector,
				subQueueConnector,collectorConnector,sampleCount));
		collectorThread.start();
		logger.debug("Subscriber:{} started its data collector thread",subId);

		//start receiver thread
		receivers.put(receiverId, new Receiver(topicName,receiverId,
				commandConnector,subQueueConnector,collectorConnector));
		receiverThreads.put(receiverId, new Thread(receivers.get(receiverId)));
		receiverId++;
		receiverThreads.get(0).start();
		logger.debug("Subscriber:{} started its receiver thread",subId);

		logger.info("Subscriber:{} will start processing incoming commands",subId );
		while (!Thread.currentThread().isInterrupted()) {
			String command = subQueueSocket.recvStr();
			if (command.equals(SUBSCRIBER_EXIT_COMMAND)) {
				logger.info("Subscriber:{} received command:{}. Will exit listener loop.",
						subId,SUBSCRIBER_EXIT_COMMAND);
				break;
			}
		}

		//Stop receiver and collector threads before exiting
		logger.debug("Subscriber:{} will stop receiver and collector threads",subId);
		commandSocket.send(String.format("%s %s",topicName,SUBSCRIBER_EXIT_COMMAND));
		try {
			for (int receiverId: receivers.keySet()) {
				Receiver receiver= receivers.get(receiverId);
				Thread receiverThread= receiverThreads.get(receiverId);

				//only interrupt receiver thread if is not connected to broker
				if(receiver.connected()==Receiver.STATE_DISCONNECTED){
					receiverThread.interrupt();
				}
				//wait for receiver thread to exit
				receiverThread.join();
			}
			//wait for collector thread to exit
			collectorThread.join();
		} catch (InterruptedException e) {
			logger.error("Subscriber:{} caught exception:{}",subId,e.getMessage());
		}
		//ZMQ set linger to 0
		commandSocket.setLinger(0);
		subQueueSocket.setLinger(0);

		//close ZMQ sockets
		commandSocket.close();
		subQueueSocket.close();

		//terminate ZMQ context
		context.term();
		
		logger.info("Subscriber:{} has exited",subId);
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
			Thread subscriber = new Thread(new Subscriber(topicName,id,sampleCount));

			//install hook to handle SIGTERM and SIGINT
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					try {
						ZMQ.Context context= ZMQ.context(1);
						ZMQ.Socket pushSocket= context.socket(ZMQ.PUSH);
						pushSocket.connect(String.format("tcp://*:%d",
								(Subscriber.SUBSCRIBER_QUEUE_BASE_PORT_NUM+id)));
						pushSocket.send(Subscriber.SUBSCRIBER_EXIT_COMMAND);
						subscriber.join();
						pushSocket.setLinger(0);
						pushSocket.close();
						context.term();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			});
			//start subscriber
			subscriber.start();
		}catch(NumberFormatException e){
			e.printStackTrace();
		}
	}
}
