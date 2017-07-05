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
	private String topicName;

	//ZMQ send/receive socket pairs
	private ZMQ.Socket receiveSocket;
	private ZMQ.Socket sendSocket;

	//Binding port numbers 
	private int receivePort;
	private int sendPort;
	
	private volatile boolean stopped= false;

	private Logger logger;
	
	public Topic(String topicName, ZMQ.Context context,int receivePort,int sendPort){
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		this.topicName= topicName;
		this.receivePort=receivePort;
		this.sendPort=sendPort;

		receiveSocket= context.socket(ZMQ.SUB);
		sendSocket= context.socket(ZMQ.PUB);

		logger.debug("Topic:%s initialized for receive port number:%d and send port number:%d",
				topicName,receivePort,sendPort);
	}

	/**
	 * Listener loop for topic thread. 
	 * Data received on receive port is forwarded to send port until 
	 * the topic thread is stopped/interrupted.
	 */
	@Override
	public void run() {
		receiveSocket.bind(String.format("tcp://*:%d",receivePort));
		receiveSocket.subscribe(topicName.getBytes());
		logger.debug("Topic:%s ZMQ.SUB socket bound to port number:%d and subscribed to topic:%s",
				topicName,receivePort,topicName);

		sendSocket.bind(String.format("tcp://*:%d",sendPort));
		logger.debug("Topic:%s ZMQ.PUB socket bound to port number:%d",
				topicName,sendPort);

		logger.debug("Topic:%s starting listener loop",topicName);

		while(!stopped && !Thread.currentThread().isInterrupted()){
			ZMsg receivedMsg = ZMsg.recvMsg(receiveSocket);
			if(receivedMsg!=null){
				String msgTopic = new String(receivedMsg.getFirst().getData());
				byte[] msgContent = receivedMsg.getLast().getData();
				sendSocket.sendMore(msgTopic);
				sendSocket.send(msgContent);
			}
		}
		receiveSocket.close();
		sendSocket.close();
		logger.debug("Topic:%s stopped",topicName);
	}

	
	public void stop(){
		stopped=true;
	}
	
	public String name(){
		return topicName;
	}
	
	
}
