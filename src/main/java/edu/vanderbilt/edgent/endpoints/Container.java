package edu.vanderbilt.edgent.endpoints;

import java.util.HashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import edu.vanderbilt.edgent.util.UtilMethods;

public abstract class Container implements Runnable{
	//Type of commands processed by the Container
	public static final String CONTAINER_EXIT_COMMAND="exit";

	//Base command and queue port numbers for Subscriber container 
	public static final int SUBSCRIBER_COMMAND_BASE_PORT_NUM=5000;
	public static final int SUBSCRIBER_QUEUE_BASE_PORT_NUM=6000;
	//Base command and queue port numbers for Publisher container 
	public static final int PUBLISHER_COMMAND_BASE_PORT_NUM=8000;
	public static final int PUBLISHER_QUEUE_BASE_PORT_NUM=9000;
	//Endpoint types
  	public static final String ENDPOINT_TYPE_SUB="sub";
  	public static final String ENDPOINT_TYPE_PUB="pub";
  	
	//ZMQ context
	protected ZMQ.Context context;
	//ZMQ.PUB socket to send control messages
	private ZMQ.Socket commandSocket;
	//ZMQ.PULL socket to receive commands for the container 
	private ZMQ.Socket queueSocket;

	//Connector for container's commandSocket
	protected String commandConnector;
	//Connector for container's queue
	protected String queueConnector;

	//Map to hold references to Workers 
	protected HashMap<Integer,Worker> workers;
	//Map to hold references to Worker threads
	protected HashMap<Integer,Thread> workerThreads;

	protected String topicName;
	protected String containerId;
	protected Logger logger;

	public Container(String topicName,String endpointType,int id){
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		this.topicName=topicName;

		containerId=String.format("%s-%s-%s-%d",endpointType,topicName,
				UtilMethods.hostName(),id);

		workers=new HashMap<Integer,Worker>();
		workerThreads=new HashMap<Integer,Thread>();

		this.context=ZMQ.context(1);

		if(endpointType.equals(ENDPOINT_TYPE_SUB)){
			commandConnector = String.format("tcp://*:%d", (SUBSCRIBER_COMMAND_BASE_PORT_NUM + id));
			queueConnector = String.format("tcp://*:%d", (SUBSCRIBER_QUEUE_BASE_PORT_NUM + id));
		}
		if(endpointType.equals(ENDPOINT_TYPE_PUB)){
			commandConnector = String.format("tcp://*:%d", (PUBLISHER_COMMAND_BASE_PORT_NUM + id));
			queueConnector = String.format("tcp://*:%d", (PUBLISHER_QUEUE_BASE_PORT_NUM + id));
		}
		logger.debug("Container:{} initialized",containerId);
	}

	@Override
	public void run() {
		logger.info("Container:{} started", containerId);

		//create and bind container's queue socket 
		queueSocket= context.socket(ZMQ.PULL);
		queueSocket.bind(queueConnector);

		//create and bind container's command socket
		commandSocket= context.socket(ZMQ.PUB);
		commandSocket.bind(commandConnector);

		//container implementation specific intialization
		initialize();

		logger.info("Container:{} will start processing incoming commands",containerId );
		while (!Thread.currentThread().isInterrupted()) {
			String command = queueSocket.recvStr();
			if (command.equals(CONTAINER_EXIT_COMMAND)) {
				logger.info("Container:{} received command:{}. Will exit listener loop.",
						containerId,CONTAINER_EXIT_COMMAND);
				break;
			}
			//TODO: process LB command
		}

		//publish CONTAINER_EXIT_COMMAND on commandSocket before exiting
		commandSocket.send(String.format("%s %s",topicName,CONTAINER_EXIT_COMMAND));
	
		//container implementation specific cleanup
		cleanup();

		//wait for all worker threads to exit
		try {
			for (int workerId: workers.keySet()) {
				Worker worker= workers.get(workerId);
				Thread workerThread= workerThreads.get(workerId);

				//only interrupt worker thread if is not connected to broker
				if(worker.connected()==Receiver.STATE_DISCONNECTED){
					workerThread.interrupt();
				}
				//wait for worker thread to exit
				workerThread.join();
			}
		} catch (InterruptedException e) {
			logger.error("Container:{} caught exception:{}",containerId,e.getMessage());
		}
		
		//ZMQ set linger to 0
		commandSocket.setLinger(0);
		queueSocket.setLinger(0);

		//close ZMQ sockets
		commandSocket.close();
		queueSocket.close();

		//terminate ZMQ context
		context.term();
		
		logger.info("Container:{} has exited",containerId);
	}
	
	public String queueConnector(){
		return queueConnector;
	}
	
	public String commandConnector(){
		return commandConnector;
	}
	
	public abstract void initialize();

	public abstract void cleanup();
}
