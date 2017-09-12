package edu.vanderbilt.edgent.endpoints;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;

import edu.vanderbilt.edgent.types.ContainerCommandHelper;
import edu.vanderbilt.edgent.util.Commands;
import edu.vanderbilt.edgent.util.UtilMethods;

public abstract class Worker implements Runnable{
	//connection states
  	public static final int STATE_CONNECTED=1;
  	public static final int STATE_DISCONNECTED=0;

  	//types of endpoints
  	public static final String ENDPOINT_TYPE_SUB="sub";
  	public static final String ENDPOINT_TYPE_PUB="pub";

  	//Polling interval
	protected static final long POLL_INTERVAL_MILISEC = 5000;

	//Maximum number of connection attempts to the same EB locator
  	private static final int MAX_RETRY_COUNT=15;

	//ZMQ context
	protected ZMQ.Context context;
	//ZMQ PUSH socket to send control messages to LbListener thread 
	private ZMQ.Socket lbSocket;
	//ZMQ SUB socket to receive control messages from parent container 
	protected ZMQ.Socket ctrlSocket;
	//ZMQ PUSH socket to send control messagesto parent container 
	protected ZMQ.Socket queueSocket;
	//ZMQ PUB/SUB socket to send/receive data from EB
	protected ZMQ.Socket socket;

	//Subscriber's socket connector at which it issues control messages
	private String controlConnector;
	/*Container's socket connector at which it receives 
	commands to enqueue in its queue */
	private String queueConnector;

	//LbListener thread to receive topic level Lb commands 
	private LbListener lbListener;
	private Thread lbListenerThread;
	//Monitor thread to monitor Receiver's connection status to EB
	private Thread monitoringThread;

	//Latch to signal connection/disconnection with EB
	private CountDownLatch connected;
	//Current state of connection with the EB
	protected AtomicInteger connectionState;
	//Number of times connection to the same EB location has been attempted
  	private AtomicInteger retryCount;
  	//Flag to indicate whether Receiver's polling loop is engaged
  	protected AtomicBoolean exited;

  	private String containerId;
  	//Type of endpoint 
  	private String endpointType;
	protected String topicName;
	//Receiver's znode location
	private String znodePath;

	//Address of EB to connect to
	private String ebId;
	private String topicConnector;
	private String ebAddress;
	//Hosting EB's exposed Topic ports 
	private String topicListenerPort;
	private String topicSendPort;
	private String topicLbPort;
	
	private String monitoringLocator;

	protected String workerId;
	protected Logger logger;

	public Worker(String containerId, int uuid,
			String topicName,String endpointType,
			String ebId,String topicConnector,
			String controlConnector, String queueConnector){
        logger= LogManager.getLogger(this.getClass().getSimpleName());
        this.containerId=containerId;
		this.topicName=topicName;
		this.endpointType=endpointType;
		this.ebId= ebId;
		this.znodePath=String.format("/eb/%s/%s/%s/%s", ebId,topicName,endpointType,containerId);
		this.topicConnector=topicConnector;
		this.controlConnector=controlConnector;
		this.queueConnector=queueConnector;

		String[] topicConnectorParts= topicConnector.split(",");
		ebAddress= topicConnectorParts[0];
		topicListenerPort= topicConnectorParts[1];
		topicSendPort= topicConnectorParts[2];
		topicLbPort= topicConnectorParts[3]; 
		
		monitoringLocator="inproc://"+ebAddress;

		workerId=String.format("%s_%s_%s_%s_%s_%d",endpointType,topicName,
				UtilMethods.hostName(),UtilMethods.pid(),ebAddress,uuid);
		connectionState= new AtomicInteger(STATE_DISCONNECTED);
		retryCount= new AtomicInteger(0);
		exited= new AtomicBoolean(false);
		logger.debug("Wroker:{} initialized",workerId);
	}

	@Override
	public void run() {
		try {
			logger.info("Worker:{} started", workerId);
			connected = new CountDownLatch(1);
			logger.info("Worker:{} will initialize ZMQ, monitoring and lb listener thread", workerId);

			// initialize ZMQ sockets
			initializeZMQ();
		
		
			//Attempt connection to EB
			if(endpointType.equals(ENDPOINT_TYPE_PUB)){
				socket.connect(String.format("tcp://%s:%s",ebAddress,topicListenerPort));
			}
			if(endpointType.equals(ENDPOINT_TYPE_SUB)){
				socket.connect(String.format("tcp://%s:%s",ebAddress,topicSendPort));
			}

			//Wait until connected to EB
			connected.await();

		    //Perform processing once connected 
			if (connectionState.get() == STATE_CONNECTED) {
				queueSocket.send(ContainerCommandHelper.serialize(Commands.CONTAINER_WORKER_CONNECTED_COMMAND,
						containerId,ebId,topicConnector));
				logger.info("Worker:{} connected to EB. Will start listening", workerId);
				process();
			}
		
			if(connectionState.get() == STATE_DISCONNECTED){
				//Send disconnected signal to parent container
				queueSocket.send(ContainerCommandHelper.serialize(Commands.CONTAINER_WORKER_DISCONNECTED_COMMAND,
						containerId, ebId, topicConnector));
			}
			if(exited.get()==true){
				//Send exited signal to parent container
				queueSocket.send(ContainerCommandHelper.serialize(Commands.CONTAINER_WORKER_EXITED_COMMAND,
						containerId, ebId, topicConnector));
			}

			cleanup();
		} catch (InterruptedException e) {
			logger.error("Worker:{} caught exception:{}",workerId,e.getMessage());
			cleanup();
		} finally {
			onExit();
		}
	}
	
	private void initializeZMQ(){
		//create ZMQ context
		context=ZMQ.context(1);
		//create ZMQ sockets
		lbSocket=context.socket(ZMQ.PUSH);
		queueSocket=context.socket(ZMQ.PUSH);
		ctrlSocket=context.socket(ZMQ.SUB);

		if(endpointType.equals(ENDPOINT_TYPE_SUB)){
			socket=context.socket(ZMQ.SUB);
		}
		if(endpointType.equals(ENDPOINT_TYPE_PUB)){
			socket=context.socket(ZMQ.PUB);
		}

		//connect/bind socket endpoints
		ctrlSocket.connect(controlConnector);
		ctrlSocket.subscribe(topicName.getBytes());
		
		String lbListenerConnector=String.format("inproc://%s", workerId);
		lbSocket.bind(lbListenerConnector);
		
		queueSocket.connect(queueConnector);

		logger.debug("Worker:{} initialized ZMQ context and sockets",workerId);

		//start LB listener thread
		lbListener=new LbListener(topicName,context,String.format("tcp://%s:%s",ebAddress,topicLbPort),
				lbListenerConnector,queueConnector,connected);
		lbListenerThread= new Thread(lbListener);
		lbListenerThread.start();

		logger.debug("Worker:{} started topic's LB listener thread",workerId);

		//start connection monitoring thread
		socket.monitor(monitoringLocator, 
				ZMQ.EVENT_CONNECTED + ZMQ.EVENT_DISCONNECTED +
				ZMQ.EVENT_CONNECT_RETRIED + ZMQ.EVENT_MONITOR_STOPPED);
		monitoringThread= new Thread(new Monitor(context,connected));
		monitoringThread.start();
		logger.debug("Worker:{} started connection monitoring thread",workerId);

		initialize();
	}

	private void cleanup(){
		logger.info("Worker:{} will cleanup ZMQ and close monitoring, LB threads",workerId);
		//close pub/sub socket
		socket.setLinger(0);
		socket.close();

		//wait for lb listener and monitor thread to exit
		try{
			// send exit signal to LB listener thread
			lbSocket.send(String.format("%s", LbListener.LB_EXIT_COMMAND));

			//if lb listener thread is waiting for connection to get established, interrupt its wait
			if(connected.getCount()>0){
				lbListenerThread.interrupt();
			}
			//wait until lb listener thread exits
			lbListenerThread.join();
			logger.debug("Worker:{} lb listener thread has exited",workerId);

			//wait until monitoring thread exits
			monitoringThread.join();
			logger.debug("Worker:{} connection monitoring thread has exited",workerId);
		}catch(InterruptedException e){
			logger.error("Worker:{} caught exception:{}",workerId,e.getMessage());
		}


		lbSocket.setLinger(0);
		lbSocket.close();
		ctrlSocket.setLinger(0);
		ctrlSocket.close();
		queueSocket.setLinger(0);
		queueSocket.close();
		
		close();

		//terminate ZMQ context
		context.term();
		logger.info("Worker:{} cleaned-up ZMQ and closed monitoring, LB threads",workerId);
	}
	

	private void onExit(){
		logger.info("Worker:{} has exited",workerId);
	}

	public int connected(){
		return connectionState.get();
	}
	
	public String znodePath(){
		return znodePath;
	}
	
	public String ebAddress(){
		return ebAddress;
	}

	public String ebId(){
		return ebId;
	}

	public abstract void initialize();
	public abstract void  process();
	public abstract void close();

	private class Monitor implements Runnable{
		private CountDownLatch connected;
		private ZMQ.Context context;
		private Logger logger;

		public Monitor(ZMQ.Context context,CountDownLatch latch){
			logger= LogManager.getLogger(this.getClass().getSimpleName());
			this.context=context;
			connected=latch;
			logger.debug("Monitor initialized");
		}

		@Override
		public void run() {
			logger.info("Monitor:{} started",Thread.currentThread().getName());
			ZMQ.Socket pair = context.socket(ZMQ.PAIR);
			pair.connect(monitoringLocator);

			while (!Thread.currentThread().isInterrupted()) {
				ZMQ.Event event = ZMQ.Event.recv(pair);

				if (event.getEvent() == ZMQ.EVENT_CONNECTED) {//Connected to EB
					logger.info("Monitor:{} connection state: connected",
							Thread.currentThread().getName());
					//set retry count to 0
					retryCount.set(0);
					//set connection state to CONNECTED
					connectionState.set(STATE_CONNECTED);
					//open waitset to allow Worker to start processing
					connected.countDown();
				}
				if (event.getEvent() == ZMQ.EVENT_CONNECT_RETRIED) {//retrying connection to EB
					logger.info("Monitor:{} connection state: retrying",
							Thread.currentThread().getName());
					//increment retry count
					int val=retryCount.getAndIncrement();
					//set connection state to DISCONNECTED 
					connectionState.set(STATE_DISCONNECTED);
					if(val>MAX_RETRY_COUNT){
						//set retry count to 0
						retryCount.set(0);
						//open waitset to allow worker to proceed. Worker will query FE to connect to another EB
						connected.countDown();
						break;
					}
				}
				if (event.getEvent() == ZMQ.EVENT_DISCONNECTED) {//disconnected from EB
					logger.info("Monitor:{} connection state: disconnected",
							Thread.currentThread().getName());
					//set retry count to 0
					retryCount.set(0);
					//set connection state to DISCONNECTED
					connectionState.set(STATE_DISCONNECTED);
					break;
				}
				if(event.getEvent()== ZMQ.EVENT_MONITOR_STOPPED){//socket being monitored was closed
					logger.info("Monitor:{} connection state: socket closed",
							Thread.currentThread().getName());
					break;
				}
			}
			pair.disconnect(monitoringLocator);
			pair.setLinger(0);
			pair.close();
			logger.info("Monitor:{} exited",
					Thread.currentThread().getName());
		}
	}
}
