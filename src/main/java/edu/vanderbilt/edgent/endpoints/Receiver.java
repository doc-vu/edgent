package edu.vanderbilt.edgent.endpoints;

import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

public class Receiver extends Worker{
	//ZMQ PUSH socket at which Receiver sends data to the collector thread 
	private ZMQ.Socket senderSocket;  
	//Connector at which collector thread listens for incoming data
	private String collectorConnector;

	public Receiver(String topicName, String endpointType, 
			int id, String controlConnector, String queueConnector,String collectorConnector) {
		super(topicName, endpointType, id, controlConnector, queueConnector);
		this.collectorConnector=collectorConnector;
	}

	@Override
	public void initialize() {
		//initialize sender socket
		senderSocket=context.socket(ZMQ.PUSH);
		senderSocket.connect(collectorConnector);
	}

	@Override
	public void process() {
		socket.subscribe(topicName.getBytes());
		ZMQ.Poller poller = context.poller(2);
		poller.register(socket, ZMQ.Poller.POLLIN);
		poller.register(ctrlSocket, ZMQ.Poller.POLLIN);

		//poll for data and control messages
		while (!Thread.currentThread().isInterrupted() &&
				connectionState.get() == STATE_CONNECTED){
			poller.poll(POLL_INTERVAL_MILISEC);
			if (poller.pollin(0)) {//process data 
				ZMsg receivedMsg = ZMsg.recvMsg(socket);
				//forward received message to collector thread
				senderSocket.send(receivedMsg.getLast().getData());
			}
			if(poller.pollin(1)){//process control message
				String command= ctrlSocket.recvStr();
				String[] args= command.split(" ");
				if(args[1].equals(Subscriber.CONTAINER_EXIT_COMMAND)){
					exited.set(true);
					break;
				}
			}
		}
	}

	@Override
	public void close() {
		//close the sender socket
		senderSocket.setLinger(0);
		senderSocket.close();
	}

}
