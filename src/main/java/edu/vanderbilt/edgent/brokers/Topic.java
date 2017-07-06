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

		logger.debug("Topic:{} initialized for receive port number:{} and send port number:{}",
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
		logger.debug("Topic:{} ZMQ.SUB socket bound to port number:{} and subscribed to topic:{}",
				topicName,receivePort,topicName);

		sendSocket.bind(String.format("tcp://*:%d",sendPort));
		logger.debug("Topic:{} ZMQ.PUB socket bound to port number:{}",
				topicName,sendPort);

		logger.info("Topic:{} thread will start listening",topicName);
		try{
			while (!stopped && !Thread.currentThread().isInterrupted()) {
				ZMsg receivedMsg = ZMsg.recvMsg(receiveSocket);
				if (receivedMsg != null) {
					String msgTopic = new String(receivedMsg.getFirst().getData());
					byte[] msgContent = receivedMsg.getLast().getData();
					sendSocket.sendMore(msgTopic);
					sendSocket.send(msgContent);
				}
			}
		}catch(Exception e){
			logger.error(e.getMessage());
		}
		logger.debug("Topic:{} thread exited", topicName);
	}


	/**
	 * Method to stop this topic thread. If this thread is not blocked on a receive
	 * call, then setting the stop flag to true will cause the thread to exit. 
	 * Otherwise, closing the receive socket will cause an exception to be thrown 
	 * and the thread will exit.  
	 */
	public void stop(){
		stopped=true;
		receiveSocket.close();
		sendSocket.close();
		logger.debug("Topic:{} receive and send sockets closed", topicName);
		logger.info("Topic:{} stopped", topicName);
	}
	
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
