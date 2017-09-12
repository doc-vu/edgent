package edu.vanderbilt.edgent.endpoints;

import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import edu.vanderbilt.edgent.types.WorkerCommand;
import edu.vanderbilt.edgent.types.WorkerCommandHelper;
import edu.vanderbilt.edgent.util.Commands;

public class Receiver extends Worker{
	//ZMQ PUSH socket at which Receiver sends data to the collector thread 
	private ZMQ.Socket senderSocket;  
	//Connector at which collector thread listens for incoming data
	private String collectorConnector;

	public Receiver(String containerId, int uuid,
			String topicName, String endpointType,
			String ebId, String topicConnector,
			String controlConnector, String queueConnector,String collectorConnector) {
		super(containerId, uuid, topicName, endpointType, ebId,topicConnector,
				controlConnector, queueConnector);
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
				logger.debug("Worker:{} received data",workerId);
				//forward received message to collector thread
				senderSocket.send(receivedMsg.getLast().getData());
			}
			if(poller.pollin(1)){//process control message
				ZMsg msg= ZMsg.recvMsg(ctrlSocket);
				WorkerCommand command=WorkerCommandHelper.deserialize(msg.getLast().getData());
				if(command.type()==Commands.CONTAINER_EXIT_COMMAND){
					exited.set(true);
					break;
				}
				if(command.type()==Commands.WORKER_EXIT_COMMAND){
					String ebId=command.ebId();
					if(ebId.equals(ebId())){
						exited.set(true);
						break;
					}
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
