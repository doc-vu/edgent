package edu.vanderbilt.edgent.endpoints;

import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

public class Sender extends Worker{
	private ZMQ.Socket producerSocket;
	private String producerConnector;

	public Sender(String topicName, String endpointType, int id,
			String controlConnector, String queueConnector,String producerConnector) {
		super(topicName, endpointType, id, controlConnector, queueConnector);
		this.producerConnector=producerConnector;
	}

	@Override
	public void initialize() {
		producerSocket=context.socket(ZMQ.SUB);
		producerSocket.connect(producerConnector);
		producerSocket.subscribe(topicName.getBytes());
	}

	@Override
	public void process() {
		ZMQ.Poller poller = context.poller(2);
		poller.register(producerSocket, ZMQ.Poller.POLLIN);
		poller.register(ctrlSocket, ZMQ.Poller.POLLIN);

		//poll for data and control messages
		while (!Thread.currentThread().isInterrupted() &&
				connectionState.get() == STATE_CONNECTED){
			poller.poll(POLL_INTERVAL_MILISEC);
			if (poller.pollin(0)) {//process data 
				ZMsg receivedMsg = ZMsg.recvMsg(producerSocket);
				socket.sendMore(receivedMsg.getFirst().getData());
				socket.send(receivedMsg.getLast().getData());
			}
			if(poller.pollin(1)){//process control message
				String command= ctrlSocket.recvStr();
				String[] args= command.split(" ");
				if(args[1].equals(Publisher.CONTAINER_EXIT_COMMAND)){
					exited.set(true);
					break;
				}
			}
		}
	}

	@Override
	public void close() {
		producerSocket.setLinger(0);
		producerSocket.close();
	}

}
