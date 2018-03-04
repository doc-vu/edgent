package edu.vanderbilt.edgent.endpoints;

import java.util.concurrent.CountDownLatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import edu.vanderbilt.edgent.types.ContainerCommand;
import edu.vanderbilt.edgent.types.ContainerCommandHelper;
import edu.vanderbilt.edgent.util.Commands;

public class LbListener implements Runnable{
	//ZMQ context
	private ZMQ.Context context;
	//ZMQ SUB socket at which topic level lb commands are received from EB
	private ZMQ.Socket subSocket;
	//ZMQ PULL socket at which control commands are received from Worker thread 
	private ZMQ.Socket controlSocket;
	//ZMQ PUSH socket to send lb commands to this worker's parent container's queue
	private ZMQ.Socket queueSocket;
	private ZMQ.Poller poller;

	//Id of container to which this LB Listener thread belongs
	private String containerId;

	//EB's topic control socket connector at which LB commands are issued
	private String ebConnector;
	//Worker's command socket at which control messages are issued
	private String controlConnector;
	//Parent container's queue in which received LB commands are sent
	private String queueConnector;

	//connection state indicator
	private CountDownLatch connected;
	private String topicName;
	private Logger logger;
	
	public LbListener(String containerId,String topicName,ZMQ.Context context,String ebConnector,
			String controlConnector,String queueConnector,
			CountDownLatch connected){
        logger= LogManager.getLogger(this.getClass().getSimpleName());
        //stash constructor arguments
        this.containerId=containerId;
		this.topicName=topicName;
		this.context=context;
		this.ebConnector=ebConnector;
		this.controlConnector=controlConnector;
		this.queueConnector=queueConnector;
		this.connected=connected;

		logger.debug("LB Listener:{} initialized",containerId);
	}

	@Override
	public void run() {
		logger.info("LB Listener:{} started",Thread.currentThread().getName());
		//initialize ZMQ sockets
		subSocket=context.socket(ZMQ.SUB);
		controlSocket=context.socket(ZMQ.PULL);
		queueSocket=context.socket(ZMQ.PUSH);

		controlSocket.connect(controlConnector);
		queueSocket.connect(queueConnector);
		
		//wait until connected to EB
		try {
			logger.info("LB Listener:{} will wait until connected to EB",containerId);
			connected.await();
		}catch(InterruptedException e){
			logger.error("LB Listener:{} caught exception:{}",containerId,e.getMessage());
			cleanup();
			return;
		}
		//Connected to EB
		if(ebConnector!=null){
			subSocket.connect(ebConnector);
			subSocket.subscribe(topicName.getBytes());

			poller = context.poller(2);
			poller.register(subSocket, ZMQ.Poller.POLLIN);
			poller.register(controlSocket, ZMQ.Poller.POLLIN);

			logger.info("LB Listener:{} will start listening",containerId);
			while (true) {
				try {
					poller.poll(-1);
					if (poller.pollin(0)) {//process LB command
						ZMsg msg= ZMsg.recvMsg(subSocket);
						byte[] data=msg.getLast().getData();
						ContainerCommand containerCommand= ContainerCommandHelper.deserialize(data);
						logger.debug("LB Listener:{} received reconf command:{}",
								containerId,containerCommand.type());
						//forward LB commands from EB to container's queue for processing
						queueSocket.send(data);
					}
					if (poller.pollin(1)) {//process control command
						String command = controlSocket.recvStr();
						if(command.equals(Commands.LB_LISTENER_EXIT_COMMAND)){
							logger.info("LB Listener:{} got control msg:{}",
									containerId,Commands.LB_LISTENER_EXIT_COMMAND);
							break;
						}
					}
				} catch (Exception e) {
					logger.error("LB Listener:{} caught exception:{}",
							containerId,e.getMessage());
					break;
				}
			}
		}
		cleanup();
		logger.info("LB Listener:{} has exited",containerId);
	}
	
	private void cleanup(){
		poller.close();
		//set linger to 0
		subSocket.setLinger(0);
		controlSocket.setLinger(0);
		queueSocket.setLinger(0);
		//close sockets
		subSocket.close();
		controlSocket.close();
		queueSocket.close();
		logger.info("LB Listener:{} closed  sockets",containerId);
	}

}
