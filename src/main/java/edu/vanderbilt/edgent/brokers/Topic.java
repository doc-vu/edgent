package edu.vanderbilt.edgent.brokers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;
/**
 * This class models a Topic/Channel hosted at an Edge/Routing Broker. 
 * Each topic is managed by a thread which creates a ZMQ.SUB socket to 
 * receive incoming data and a ZMQ.PUB socket to send data out to subscribers.
 * @author kharesp
 */
public class Topic implements Runnable {
	//Port at which hosting broker issues topic control messages
	public static final int TOPIC_CONTROL_PORT=20126;
	//Topic control messages 
	public static final String TOPIC_DELETE_COMMAND="delete";

	//Topic name
	private String topicName;

	//ZMQ SUB socket to receive control messages from hosting broker
	private ZContext context;
	private ZMQ.Socket topicControl;
	//ZMQ send/receive socket pairs
	private ZMQ.Socket receiveSocket;
	private ZMQ.Socket sendSocket;

	//poller to poll receiveSocket for data and topicControl for control msgs
	private ZMQ.Poller poller;

	//Binding port numbers for receiveSocket and sendSocket 
	private int receivePort;
	private int sendPort;

	private Logger logger;
	
	public Topic(String topicName, ZContext context,int receivePort,int sendPort){
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		this.topicName= topicName;
		this.receivePort=receivePort;
		this.sendPort=sendPort;
		this.context=context;

		//instantiate ZMQ Sockets and poller
		topicControl=context.createSocket(ZMQ.SUB);
		receiveSocket= context.createSocket(ZMQ.SUB);
		sendSocket= context.createSocket(ZMQ.PUB);
		poller=context.createPoller(2);

		logger.debug("Topic:{} initialized for receive port number:{} and send port number:{}",
				topicName,receivePort,sendPort);
	}

	/**
	 * Listener loop for topic thread. 
	 * Data received on receive port is forwarded to send port until 
	 * the topic thread is stopped via control message received on topicControl socket
	 */
	@Override
	public void run() {
		//connect topicControl socket to hosting broker's TOPIC_CONTROL_PORT
		topicControl.connect(String.format("tcp://localhost:%d",TOPIC_CONTROL_PORT));
		//subscribe to receive topic control messages
		topicControl.subscribe(topicName.getBytes());

		//bind receiveSocket to receivePort and subscribe to receive topic's data
		receiveSocket.bind(String.format("tcp://*:%d",receivePort));
		receiveSocket.subscribe(topicName.getBytes());
		logger.debug("Topic:{} ZMQ.SUB socket bound to port number:{} and subscribed to topic:{}",
				topicName,receivePort,topicName);
	
		//register receiveSocket and topicControl with the poller
		poller.register(receiveSocket, ZMQ.Poller.POLLIN);
		poller.register(topicControl, ZMQ.Poller.POLLIN);

		//bind sendSocket to sendPort to send topic's data 
		sendSocket.bind(String.format("tcp://*:%d",sendPort));
		logger.debug("Topic:{} ZMQ.PUB socket bound to port number:{}",
				topicName,sendPort);
	
		// topic thread's listener loop
		logger.info("Topic:{} thread will start listening", topicName);
		while (!Thread.currentThread().isInterrupted()) {
			try {
				// block until either topicControl or receiveSocket have data
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
				// in case topicControl has data
				if (poller.pollin(1)) {
					String[] data= topicControl.recvStr().split(" ");
					System.out.println(data[1]);
					logger.debug("Topic:{} received control msg:{}", topicName, data[1]);
					if(data[1].equals(TOPIC_DELETE_COMMAND)){
						break;
					}
				}
			}catch (ZMQException e) {
				logger.error(e.getMessage());
				break;
			}catch(Exception e){
				logger.error(e.getMessage());
				break;
			}
		}
		// clean up before exiting
		receiveSocket.close();
		sendSocket.close();
		topicControl.close();
		context.destroy();
		logger.info("Topic:{} deleted", topicName);
	}

	//Accessors for topic Name,receivePort, sendPort 
	public String name(){
		return topicName;
	}
	
	public int receivePort(){
		return receivePort;
	}
	
	public int sendPort(){
		return sendPort;
	}
}
