package edu.vanderbilt.edgent.endpoints.subscriber;

import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import edu.vanderbilt.edgent.endpoints.Worker;
import edu.vanderbilt.edgent.types.WorkerCommand;
import edu.vanderbilt.edgent.types.WorkerCommandHelper;
import edu.vanderbilt.edgent.util.Commands;

public class Receiver extends Worker{
	//ZMQ PUSH socket at which Receiver sends data to the collector thread 
	private ZMQ.Socket senderSocket;  
	//Connector at which collector thread listens for incoming data
	private String collectorConnector;
	

	public Receiver(ZMQ.Context context,String containerId, int uuid,
			String topicName, String endpointType,
			String ebId, String topicConnector,
			String controlConnector, String queueConnector,String collectorConnector) {
		super(context,containerId, uuid, topicName, endpointType, ebId,topicConnector,
				controlConnector, queueConnector,0);
		this.collectorConnector=collectorConnector;
	}

	@Override
	public void initialize() {
		//initialize sender socket
		senderSocket=context.socket(ZMQ.PUB);
		senderSocket.setHWM(0);
		senderSocket.connect(collectorConnector);
	}
	
	@Override
	public void process(){
		receiveData();
	}
	
	public void receiveData(){
		socket.subscribe(topicName.getBytes());
		ZMQ.Poller poller = context.poller(2);
		poller.register(socket, ZMQ.Poller.POLLIN);
		poller.register(ctrlSocket, ZMQ.Poller.POLLIN);

		boolean disconnected=false;
		int pollAttempt=0;
		//poll for data and control messages
		while (!Thread.currentThread().isInterrupted() &&
				connectionState.get() == WORKER_STATE_CONNECTED){
			poller.poll(POLL_INTERVAL_MILISEC);
			
			if (poller.pollin(0)) {//process data 
				ZMsg receivedMsg = ZMsg.recvMsg(socket);
				//forward received message to collector thread
				senderSocket.sendMore(ebId().getBytes());
				senderSocket.send(receivedMsg.getLast().getData());
				if(disconnected){
					pollAttempt=0;
				}
			}
			if(!poller.pollin(0) && disconnected){
				pollAttempt++;
				if(pollAttempt==10){
					exited.set(true);
					break;
				}
			}
			if(poller.pollin(1)){//process control message
				ZMsg msg= ZMsg.recvMsg(ctrlSocket);
				WorkerCommand command=WorkerCommandHelper.deserialize(msg.getLast().getData());
				if(command.type()==Commands.CONTAINER_EXIT_COMMAND){
					exited.set(true);
					break;
				}
				if(command.type()==Commands.WORKER_EXIT_IMMEDIATELY_COMMAND){
					String ebId=command.topicConnector().ebId();
					if(ebId.equals(ebId())){
						disconnected=true;
						exited.set(true);
						break;
					}
				}
			}
		}
		poller.close();
	}

	@Override
	public void close() {
		//close the sender socket
		senderSocket.setLinger(0);
		senderSocket.close();
	}

}
