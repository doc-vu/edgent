package edu.vanderbilt.edgent.brokers;

import java.util.concurrent.CountDownLatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;
import edu.vanderbilt.edgent.types.ContainerCommand;
import edu.vanderbilt.edgent.types.ContainerCommandHelper;
import edu.vanderbilt.edgent.types.TopicCommand;
import edu.vanderbilt.edgent.types.TopicCommandHelper;
import edu.vanderbilt.edgent.util.UtilMethods;
/**
 * This class models a Topic/Channel hosted at an Edge/Routing Broker. 
 * Each topic is managed by a thread which creates a ZMQ.SUB socket to 
 * receive incoming data and a ZMQ.PUB socket to send data out to subscribers.
 * @author kharesp
 */
public class Topic implements Runnable {
	//Types of Topic level commands    
	public static final int TOPIC_DELETE_COMMAND=0;
	public static final int TOPIC_LB_COMMAND=1;

	//Topic name
	private String topicName;
	//ZMQ context
	private ZMQ.Context context;
	//ZMQ SUB socket to receive control messages from hosting broker
	private ZMQ.Socket controlSocket;
	//ZMQ SUB socket to receive topic data 
	private ZMQ.Socket receiveSocket;
	//ZMQ PUB socket to send topic data
	private ZMQ.Socket sendSocket;
	//ZMQ PUB socket to send LB commands for this topic
	private ZMQ.Socket lbSocket;

	//poller to poll receiveSocket for data and controlSocket for control msgs
	private ZMQ.Poller poller;

	//Binding port numbers for receiveSocket,sendSocket and lbSocket
	private int receivePort;
	private int sendPort;
	private int lbPort;
	private String topicConnector;
	//EB's socket connector at which topic control commands are issued
	private String topicControlConnector;

	private boolean listening;
	private CountDownLatch initialized;
	private Logger logger;
	
	public Topic(String topicName, ZMQ.Context context,String topicControlConnector,
			int receivePort,int sendPort,int lbPort,CountDownLatch initialized){
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		this.topicName= topicName;
		this.context=context;
		this.topicControlConnector=topicControlConnector;
		this.receivePort=receivePort;
		this.sendPort=sendPort;
		this.lbPort=lbPort;
		this.initialized=initialized;
		
		topicConnector=String.format("%s,%d,%d,%d",UtilMethods.ipAddress(),
				receivePort,sendPort,lbPort);
		listening=false;

		logger.info("Topic:{} initialized for receive port:{} send port:{} and lb port:{} ",
				topicName,receivePort,sendPort,lbPort);
	}

	/**
	 * Listener loop for topic thread. 
	 * Data received on receive port is forwarded to send port until 
	 * the topic thread is stopped via control message received on controlSocket
	 */
	@Override
	public void run() {
		//instantiate ZMQ Sockets and poller
		controlSocket=context.socket(ZMQ.SUB);
		receiveSocket= context.socket(ZMQ.SUB);
		sendSocket= context.socket(ZMQ.PUB);
		lbSocket= context.socket(ZMQ.PUB);
		poller=context.poller(2);

		try{
			// connect control socket to hosting broker's TOPIC_CONTROL_PORT
			controlSocket.connect(topicControlConnector);
			// subscribe to receive topic control messages
			controlSocket.subscribe(topicName.getBytes());

			// bind receiveSocket to receivePort and subscribe to receive
			// topic's data
			receiveSocket.bind(String.format("tcp://*:%d", receivePort));
			receiveSocket.subscribe(topicName.getBytes());
			logger.debug("Topic:{} receive socket bound to port:{} and subscribed to topic:{}", topicName, receivePort,
					topicName);

			// register receiveSocket and controlSocket with the poller
			poller.register(receiveSocket, ZMQ.Poller.POLLIN);
			poller.register(controlSocket, ZMQ.Poller.POLLIN);

			// bind sendSocket to sendPort to send topic's data
			sendSocket.bind(String.format("tcp://*:%d", sendPort));
			logger.debug("Topic:{} send socket bound to port number:{}", topicName, sendPort);

			// bind lbSocket to lbPort to send LB commands for this topic
			lbSocket.bind(String.format("tcp://*:%d", lbPort));
			logger.debug("Topic:{}  LB socket bound to port number:{}", topicName, lbPort);
		}catch(ZMQException e){
			logger.error("Topic:{} caught exception:{}",topicName,e.getMessage());
			initialized.countDown();
			cleanup();
			return;
		}
	
		listening=true;
		initialized.countDown();


		// topic thread's listener loop
		logger.info("Topic:{} thread will start listening", topicName);
		while (!Thread.currentThread().isInterrupted()) {
			try {
				// block until either controlSocket or receiveSocket have data
				poller.poll(-1);

				// in case receiveSocket has data
				if (poller.pollin(0)) {
					ZMsg receivedMsg = ZMsg.recvMsg(receiveSocket);
					String msgTopic = new String(receivedMsg.getFirst().getData());
					byte[] msgContent = receivedMsg.getLast().getData();
					sendSocket.sendMore(msgTopic);
					sendSocket.send(msgContent);
					logger.debug("Topic:{} received data", topicName);
				}
				// in case controlSocket has data
				if (poller.pollin(1)) {
					ZMsg controlMsg= ZMsg.recvMsg(controlSocket);
					String topicName= new String(controlMsg.getFirst().getData());
					TopicCommand command= TopicCommandHelper.deserialize(controlMsg.getLast().getData());
					if(command.type()== Topic.TOPIC_DELETE_COMMAND){
						logger.debug("Topic:{} received TOPIC_DELETE_COMMAND",topicName);
						break;
					}
					if(command.type()== Topic.TOPIC_LB_COMMAND){
						ContainerCommand containerCommand= command.containerCommand();
						lbSocket.sendMore(topicName.getBytes());
						lbSocket.send(ContainerCommandHelper.serialize(containerCommand));
						logger.debug("Topic:{} send LB control message to all connected endpoints",topicName);
					}
				}
			}catch(Exception e){
				logger.error("Topic:{} caught exception:{}",
						topicName,e.getMessage());
				break;
			}
		}
		cleanup();
		logger.info("Topic:{} deleted", topicName);
	}
	
	private void cleanup(){
		//set linger to 0
		controlSocket.setLinger(0);
		receiveSocket.setLinger(0);
		sendSocket.setLinger(0);
		lbSocket.setLinger(0);
		//close sockets
		controlSocket.close();
		receiveSocket.close();
		sendSocket.close();
		lbSocket.close();
	}

	//Accessors for topic Name,receivePort, sendPort, lbPort
	public String name(){
		return topicName;
	}
	
	public int receivePort(){
		return receivePort;
	}
	
	public int sendPort(){
		return sendPort;
	}

	public int lbPort(){
		return lbPort;
	}
	
	public boolean listening(){
		return listening;
	}
	
	public String topicConnector(){
		return topicConnector;
	}
	
}
