package edu.vanderbilt.edgent.subscriber;

import java.util.concurrent.CountDownLatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;

public class LbListener implements Runnable{
	public static final String LB_EXIT_COMMAND="exit";
	//ZMQ context
	private ZMQ.Context context;
	//ZMQ SUB socket at which topic level lb commands are received from EB
	private ZMQ.Socket subSocket;
	//ZMQ PULL socket at which control commands are received from Receiver  
	private ZMQ.Socket controlSocket;
	//ZMQ PUSH socket to send lb commands to subscriber container
	private ZMQ.Socket commandSocket;

	//EB's topic control socket connector at which LB commands are issued
	private String ebConnector;
	//Receiver's command socket at which control messages are issued
	private String controlConnector;
	//Subscriber container's queue in which received LB commands are sent
	private String subQueueConnector;

	//connection state indicator
	private CountDownLatch connected;
	private String topicName;
	private Logger logger;
	
	public LbListener(String topicName,ZMQ.Context context,
			String controlConnector,String subQueueConnector,
			CountDownLatch connected){
        logger= LogManager.getLogger(this.getClass().getSimpleName());
		this.topicName=topicName;
		this.context=context;
		ebConnector=null;
		this.controlConnector=controlConnector;
		this.subQueueConnector=subQueueConnector;
		this.connected=connected;

		logger.debug("LB Listener initialized");
	}

	@Override
	public void run() {
		logger.info("LB Listener:{} started",Thread.currentThread().getName());
		subSocket=context.socket(ZMQ.SUB);
		controlSocket=context.socket(ZMQ.PULL);
		commandSocket=context.socket(ZMQ.PUSH);

		controlSocket.connect(controlConnector);
		commandSocket.connect(subQueueConnector);
		try {
			logger.info("LB Listener:{} will wait until connected to EB",Thread.currentThread().getName());
			connected.await();
		}catch(InterruptedException e){
			logger.error("LB Listener:{} caught exception:{}",
					Thread.currentThread().getName(),e.getMessage());
			cleanup();
			return;
		}
		if(ebConnector!=null){
			subSocket.connect(ebConnector);
			subSocket.subscribe(topicName.getBytes());

			ZMQ.Poller poller = context.poller(2);
			poller.register(subSocket, ZMQ.Poller.POLLIN);
			poller.register(controlSocket, ZMQ.Poller.POLLIN);

			logger.info("LB Listener:{} will start listening",
					Thread.currentThread().getName());
			while (true) {
				try {
					poller.poll(-1);
					if (poller.pollin(0)) {//process LB command
						String reconfCommand = subSocket.recvStr();
						//forward LB commands from EB to sub queue
						commandSocket.send(reconfCommand);
					}
					if (poller.pollin(1)) {//process control command
						String command = controlSocket.recvStr();
						if(command.equals(LB_EXIT_COMMAND)){
							break;
						}
					}
				} catch (Exception e) {
					logger.error("LB Listener:{} caught exception:{}",
							Thread.currentThread().getName(),e.getMessage());
					break;
				}
			}
		}
		cleanup();
		logger.info("LB Listener:{} has exited",Thread.currentThread().getName());
	}
	
	private void cleanup(){
		//set linger to 0
		subSocket.setLinger(0);
		controlSocket.setLinger(0);
		commandSocket.setLinger(0);
		//close sockets
		subSocket.close();
		controlSocket.close();
		commandSocket.close();
		logger.info("LB Listener:{} closed ZMQ context and sockets",
				Thread.currentThread().getName());
	}

	public void setTopicControlLocator(String ebConnector){
		this.ebConnector=ebConnector;
	}
}
