package edu.vanderbilt.edgent.endpoints;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import edu.vanderbilt.edgent.fe.Frontend;
import edu.vanderbilt.edgent.util.UtilMethods;

public abstract class Worker implements Runnable{
  	public static final int STATE_CONNECTED=1;
  	public static final int STATE_DISCONNECTED=0;

  	public static final String WORKER_EXIT_COMMAND="exit";

  	public static final String ENDPOINT_TYPE_SUB="sub";
  	public static final String ENDPOINT_TYPE_PUB="pub";

	protected static final long POLL_INTERVAL_MILISEC = 5000;
	//Endpoint at which monitoring is done 
	private static final String CONNECTION_MONITORING_LOCATOR="inproc://monitor";
  	private static final int MAX_RETRY_COUNT=15;

	//ZMQ context
	protected ZMQ.Context context;
	//ZMQ REQ socket to query FE 
	private ZMQ.Socket feSocket;
	//ZMQ PUSH socket to send control messages to LbListener thread 
	private ZMQ.Socket lbSocket;
	//ZMQ SUB socket to receive control messages from Subscriber 
	protected ZMQ.Socket ctrlSocket;
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

  	//Type of endpoint 
  	private String endpointType;
	protected String topicName;
	//Receiver's znode location
	private String znodePath;
	//Address of EB to connect to
	private String ebAddress;
	//Hosting EB's exposed Topic ports 
	private String topicListenerPort;
	private String topicSendPort;
	private String topicLbPort;

	private String id;
	private Logger logger;

	public Worker(String topicName,String endpointType,
			int id, String controlConnector,
			String queueConnector){
        logger= LogManager.getLogger(this.getClass().getSimpleName());
		this.topicName=topicName;
		this.endpointType=endpointType;
		this.controlConnector=controlConnector;
		this.queueConnector=queueConnector;

		this.id=String.format("%s_%s_%s_%s_%d",endpointType,topicName,
				UtilMethods.hostName(),UtilMethods.pid(),id);
		connectionState= new AtomicInteger(STATE_DISCONNECTED);
		retryCount= new AtomicInteger(0);
		exited= new AtomicBoolean(false);
		logger.debug("Wroker:{} initialized",id);
	}

	@Override
	public void run() {
		try {
			logger.info("Receiver:{} started",id);
			while (!Thread.currentThread().isInterrupted() && !exited.get()) {
				connected = new CountDownLatch(1);
				logger.info("Receiver:{} will initialize ZMQ, monitoring and lb listener thread",id);
				initializeZMQ();
				logger.info("Receiver:{} will query FE for hosting EB's location",id);
				String ebLocator = queryFe();
				if (ebLocator == null) {
					logger.info("Receiver:{} received invalid EB address",id);
					cleanup();
					break;
				} else {
					logger.info("Receiver:{} received EB address:{}. Will atttempt connecting to EB",id,ebLocator);
					socket.connect(ebLocator);
					connected.await();
					if (connectionState.get() == STATE_CONNECTED) {
						logger.info("Receiver:{} connected to EB:{}. Will start listening",id,ebLocator);
						process();
					}
					cleanup();
				}
			}
		} catch (InterruptedException e) {
			logger.error("Receiver:{} caught exception:{}",id,e.getMessage());
			cleanup();
		} finally {
			onExit();
		}
	}
	
	private void initializeZMQ(){
		//create ZMQ context
		context=ZMQ.context(1);
		//create ZMQ sockets
		feSocket=context.socket(ZMQ.REQ);
		lbSocket=context.socket(ZMQ.PUSH);
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
		
		System.out.println(UtilMethods.ipAddress());
		System.out.println(UtilMethods.regionId());

		String feAddress=Frontend.FE_LOCATIONS.get(UtilMethods.regionId());
		System.out.println("FE address:"+feAddress);
		feSocket.connect(String.format("tcp://%s:%d",feAddress,Frontend.LISTENER_PORT));

		String lbListenerConnector=String.format("inproc://%s", id);
		lbSocket.bind(lbListenerConnector);

		logger.debug("Receiver:{} initialized ZMQ context and sockets",id);

		//start LB listener thread
		lbListener=new LbListener(topicName,context,
				lbListenerConnector,queueConnector,connected);
		lbListenerThread= new Thread(lbListener);
		lbListenerThread.start();

		logger.debug("Receiver:{} started topic's LB listener thread",id);

		//start connection monitoring thread
		socket.monitor(CONNECTION_MONITORING_LOCATOR, 
				ZMQ.EVENT_CONNECTED + ZMQ.EVENT_DISCONNECTED +
				ZMQ.EVENT_CONNECT_RETRIED + ZMQ.EVENT_MONITOR_STOPPED);
		monitoringThread= new Thread(new Monitor(context,connected));
		monitoringThread.start();
		
		initialize();
		
		logger.debug("Receiver:{} started connection monitoring thread",id);
	}
	
	public abstract void initialize();

	private String queryFe(){
		//query FE for hosting EB's location
		logger.info("Receiver:{} will query FE for hosting EB's location",id);
		feSocket.send(String.format("%s,%s,%s,%s,%s",
				Frontend.CONNECTION_REQUEST,
				topicName,
				"sub",
				UtilMethods.ipAddress(),
				znodePath));
		String res= feSocket.recvStr();

		String ebLocator=null;
		if(res.startsWith("Error")){
			logger.error("Receiver:{} FE responded with error:{}",id,res);
		}else{
			//parse FE query result
			String[] parts= res.split(";");
			//EB location
			String[] locatorParts = parts[0].split(",");
			ebAddress=locatorParts[0];
			topicListenerPort=locatorParts[1];
			topicSendPort=locatorParts[2];
			topicLbPort=locatorParts[3];
			//Receiver's created znode path
			znodePath = parts[1];

			logger.info("Receiver:{} FE query result: EB:{} znode:{}",
					id,parts[0],parts[1]);
	
			if(endpointType.equals(ENDPOINT_TYPE_SUB)){
				ebLocator=String.format("tcp://%s:%s", ebAddress, topicSendPort);
			}
			if(endpointType.equals(ENDPOINT_TYPE_PUB)){
				ebLocator=String.format("tcp://%s:%s", ebAddress, topicListenerPort);
			}
			
		}
		return ebLocator;
	}
	
	public abstract void  process();

	private void cleanup(){
		logger.info("Receiver:{} will cleanup ZMQ and close monitoring, LB threads",id);
		//close subscriber socket
		socket.setLinger(0);
		socket.close();
		//send exit signal to LB listener thread
		lbSocket.send(String.format("%s",LbListener.LB_EXIT_COMMAND));
		try{
			//if lb listener thread is waiting for connection to get established, interrupt its wait
			if(connected.getCount()>0){
				lbListenerThread.interrupt();
			}
			//wait until lb listener thread exits
			lbListenerThread.join();
			logger.debug("Receiver:{} lb listener thread has exited",id);
			//wait until monitoring thread exits
			monitoringThread.join();
			logger.debug("Receiver:{} connection monitoring thread has exited",id);
		}catch(InterruptedException e){
			logger.error("Receiver:{} caught exception:{}",id,e.getMessage());
		}
		//Request FE to remove this endpoint's znode
		feSocket.send(String.format("%s,%s", Frontend.DISCONNECTION_REQUEST, znodePath));
		//close all ZMQ sockets
		feSocket.setLinger(0);
		feSocket.close();
		lbSocket.setLinger(0);
		lbSocket.close();
		ctrlSocket.setLinger(0);
		ctrlSocket.close();
		
		close();
		//terminate ZMQ context
		context.term();

		logger.info("Receiver:{} cleaned-up ZMQ and closed monitoring, LB threads",id);
	}
	
	public abstract void close();

	private void onExit(){
		logger.info("Receiver:{} has exited",id);
	}

	public int connected(){
		return connectionState.get();
	}

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
			pair.connect(CONNECTION_MONITORING_LOCATOR);

			while (!Thread.currentThread().isInterrupted()) {
				ZMQ.Event event = ZMQ.Event.recv(pair);

				if (event.getEvent() == ZMQ.EVENT_CONNECTED) {//Connected to EB
					logger.info("Monitor:{} connection state: connected",
							Thread.currentThread().getName());
					//set retry count to 0
					retryCount.set(0);
					//set connection state to CONNECTED
					connectionState.set(STATE_CONNECTED);
					//set EB connector for LB listener thread 
					lbListener.setTopicControlLocator(String.format("tcp://%s:%s",ebAddress,topicLbPort));
					//open waitset to allow client to start processing
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
						//open waitset to allow client to proceed. Client will query FE to connect to another EB
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
			pair.disconnect(CONNECTION_MONITORING_LOCATOR);
			pair.setLinger(0);
			pair.close();
			logger.info("Monitor:{} exited",
					Thread.currentThread().getName());
		}
	}
}
