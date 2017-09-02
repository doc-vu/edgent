package edu.vanderbilt.edgent.brokers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
/**
 * This class models a Topic/Channel hosted at an Edge/Routing Broker. 
 * Each topic is managed by a thread which creates a ZMQ.SUB socket to 
 * receive incoming data and a ZMQ.PUB socket to send data out to subscribers.
 * @author kharesp
 */
public class Topic implements Runnable {
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

	private Logger logger;
	
	public Topic(String topicName, ZMQ.Context context,
			int receivePort,int sendPort,int lbPort){
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		this.topicName= topicName;
		this.context=context;
		this.receivePort=receivePort;
		this.sendPort=sendPort;
		this.lbPort=lbPort;

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

		//connect control socket to hosting broker's TOPIC_CONTROL_PORT
		controlSocket.connect(String.format("tcp://localhost:%d",EdgeBroker.TOPIC_CONTROL_PORT));
		//subscribe to receive topic control messages
		controlSocket.subscribe(topicName.getBytes());

		//bind receiveSocket to receivePort and subscribe to receive topic's data
		receiveSocket.bind(String.format("tcp://*:%d",receivePort));
		receiveSocket.subscribe(topicName.getBytes());
		logger.debug("Topic:{} receive socket bound to port:{} and subscribed to topic:{}",
				topicName,receivePort,topicName);
	
		//register receiveSocket and controlSocket with the poller
		poller.register(receiveSocket, ZMQ.Poller.POLLIN);
		poller.register(controlSocket, ZMQ.Poller.POLLIN);

		//bind sendSocket to sendPort to send topic's data 
		sendSocket.bind(String.format("tcp://*:%d",sendPort));
		logger.debug("Topic:{} send socket bound to port number:{}",
				topicName,sendPort);

		//bind lbSocket to lbPort to send LB commands for this topic
		lbSocket.bind(String.format("tcp://*:%d",lbPort));
		logger.debug("Topic:{}  LB socket bound to port number:{}",
				topicName,lbPort);
	
		// topic thread's listener loop
		logger.info("Topic:{} thread will start listening", topicName);
		while (!Thread.currentThread().isInterrupted()) {
			try {
				// block until either controlSocket or receiveSocket have data
				poller.poll(-1);

				// in case receiveSocket has data
				if (poller.pollin(0)) {
					ZMsg receivedMsg = ZMsg.recvMsg(receiveSocket);
					if (receivedMsg != null) {
						String msgTopic = new String(receivedMsg.getFirst().getData());
						byte[] msgContent = receivedMsg.getLast().getData();
						sendSocket.sendMore(msgTopic);
						sendSocket.send(msgContent);
					}
				}
				// in case controlSocket has data
				if (poller.pollin(1)) {
					String[] data= controlSocket.recvStr().split(" ");
					logger.debug("Topic:{} received control msg:{}", topicName, data[1]);
					if(data[1].equals(EdgeBroker.TOPIC_DELETE_COMMAND)){
						break;
					}
					if(data[1].equals(EdgeBroker.TOPIC_LB_COMMAND)){
						lbSocket.send(String.format("%s lb",topicName));
					}
				}
			}catch(Exception e){
				logger.error("Topic:{} caught exception:{}",
						topicName,e.getMessage());
				break;
			}
		}
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

		logger.info("Topic:{} deleted", topicName);
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
}
