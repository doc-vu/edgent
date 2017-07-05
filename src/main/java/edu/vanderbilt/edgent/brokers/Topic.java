package edu.vanderbilt.edgent.brokers;

import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

public class Topic implements Runnable {
	private String topic;

	private ZMQ.Context context;
	private ZMQ.Socket receiveSocket;
	private ZMQ.Socket sendSocket;

	private int receivePort;
	private int sendPort;
	
	private volatile boolean stopped= false;
	
	public Topic(String topic, ZMQ.Context context,int receivePort,int sendPort){
		this.topic= topic;

		this.context=context;
		receiveSocket= context.socket(ZMQ.SUB);
		sendSocket= context.socket(ZMQ.PUB);

		this.receivePort=receivePort;
		this.sendPort=sendPort;
	}

	@Override
	public void run() {
		receiveSocket.bind(String.format("tcp://*:%d",receivePort));
		receiveSocket.subscribe(topic.getBytes());

		sendSocket.bind(String.format("tcp://*:%d",sendPort));
		while(!stopped){
			ZMsg receivedMsg = ZMsg.recvMsg(receiveSocket);
			String msgTopic = new String(receivedMsg.getFirst().getData());
		    byte[] msgContent = receivedMsg.getLast().getData();
		    sendSocket.sendMore(msgTopic);
	        sendSocket.send(msgContent);
		}
		receiveSocket.close();
		sendSocket.close();
	}

	
	public void stop(){
		stopped=true;
	}
	
}
