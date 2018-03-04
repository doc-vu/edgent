package edu.vanderbilt.edgent.endpoints.publisher;

import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import edu.vanderbilt.edgent.endpoints.Worker;
import edu.vanderbilt.edgent.types.WorkerCommand;
import edu.vanderbilt.edgent.types.WorkerCommandHelper;
import edu.vanderbilt.edgent.util.Commands;

public class Sender extends Worker{
	//ZMQ SUB socket at which Sender thread listens for data produced by the Producer thread
	private ZMQ.Socket receiverSocket;
	//Connector at which the producer thread publishes data
	private String producerConnector;

	public Sender(ZMQ.Context context,String containerId, int uuid,
			String topicName, String endpointType,
			String ebId,String topicConnector,
			String controlConnector, String queueConnector,String producerConnector) {
		super(context,containerId, uuid,topicName, endpointType,ebId,topicConnector,
				controlConnector, queueConnector,0);
		this.producerConnector=producerConnector;
	}

	@Override
	public void initialize() {
		//instantiate the receiver socket
		receiverSocket=context.socket(ZMQ.SUB);
		receiverSocket.connect(producerConnector);
		receiverSocket.subscribe(topicName.getBytes());
	}

	@Override
	public void process() {
		ZMQ.Poller poller = context.poller(2);
		poller.register(receiverSocket, ZMQ.Poller.POLLIN);
		poller.register(ctrlSocket, ZMQ.Poller.POLLIN);

		//poll for data and control messages
		while (!Thread.currentThread().isInterrupted() &&
				connectionState.get() == WORKER_STATE_CONNECTED){
			poller.poll(POLL_INTERVAL_MILISEC);
			if (poller.pollin(0)) {//process data 
				ZMsg receivedMsg = ZMsg.recvMsg(receiverSocket);
				socket.sendMore(receivedMsg.getFirst().getData());
				socket.send(receivedMsg.getLast().getData());
			}
			if(poller.pollin(1)){//process control message
				ZMsg msg= ZMsg.recvMsg(ctrlSocket);
				WorkerCommand command= WorkerCommandHelper.deserialize(msg.getLast().getData());

				if(command.type()==Commands.CONTAINER_EXIT_COMMAND){
					exited.set(true);
					break;
				}
				if(command.type()==Commands.WORKER_EXIT_COMMAND){
					String ebId=command.topicConnector().ebId();
					if(ebId.equals(ebId())){
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
		//close receiver socket
		receiverSocket.setLinger(0);
		receiverSocket.close();
	}

}
