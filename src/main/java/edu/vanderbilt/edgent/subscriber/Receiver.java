package edu.vanderbilt.edgent.subscriber;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import edu.vanderbilt.edgent.fe.Frontend;
import edu.vanderbilt.edgent.util.UtilMethods;

public class Receiver implements Runnable{
	//Endpoint at which monitoring is done 
	private static final String CONNECTION_MONITORING_LOCATOR="inproc://monitor";
  	private static final int MAX_RETRY_COUNT=15;
  	private static final int POLL_INTERVAL_MILISEC=5000;
  	public static final int STATE_CONNECTED=1;
  	public static final int STATE_DISCONNECTED=0;

	//ZMQ context
	private ZMQ.Context context;
	//ZMQ REQ socket to query FE 
	private ZMQ.Socket feSocket;
	//ZMQ PUSH socket to send control messages to LbListener thread 
	private ZMQ.Socket lbSocket;
	//ZMQ SUB socket to receive topic data from EB
	private ZMQ.Socket subSocket;
	//ZMQ SUB socket to receive control messages from Subscriber 
	private ZMQ.Socket ctrlSocket;
	//ZMQ PUSH socket to send data to collector thread 
	private ZMQ.Socket collectorSocket;

	//Subscriber's socket connector at which it issues control messages
	private String controlConnector;
	/*Subscriber container's socket connector at which it receives 
	commands to enqueue in its queue */
	private String subQueueConnector;
	//Collector thread's socket connector at which it receives data 
	private String collectorConnector;

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
  	private AtomicBoolean exited;
  	
	private String topicName;
	//Receiver's znode location
	private String znodePath;
	//Address of EB to connect to
	private String ebAddress;
	@SuppressWarnings("unused")
	//Hosting EB's exposed Topic ports 
	private String topicListenerPort;
	private String topicSendPort;
	private String topicLbPort;

	private String id;
	private Logger logger;

	public Receiver(String topicName,int id,
			String controlConnector,
			String subQueueConnector,
			String collectorConnector){
        logger= LogManager.getLogger(this.getClass().getSimpleName());
		this.topicName=topicName;
		this.controlConnector=controlConnector;
		this.subQueueConnector=subQueueConnector;
		this.collectorConnector=collectorConnector;

		this.id=String.format("sub_%s_%s_%s_%d",topicName,
				UtilMethods.hostName(),UtilMethods.pid(),id);
		connectionState= new AtomicInteger(STATE_DISCONNECTED);
		retryCount= new AtomicInteger(0);
		exited= new AtomicBoolean(false);
		logger.debug("Receiver:{} initialized",id);
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
					subSocket.connect(ebLocator);
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
		subSocket=context.socket(ZMQ.SUB);
		ctrlSocket=context.socket(ZMQ.SUB);
		collectorSocket=context.socket(ZMQ.PUSH);

		//connect/bind socket endpoints
		ctrlSocket.connect(controlConnector);
		ctrlSocket.subscribe(topicName.getBytes());
		collectorSocket.connect(collectorConnector);

		String feAddress=Frontend.FE_LOCATIONS.get(UtilMethods.regionId());
		feSocket.connect(String.format("tcp://%s:%d",feAddress,Frontend.LISTENER_PORT));

		String lbListenerConnector=String.format("inproc://%s", id);
		lbSocket.bind(lbListenerConnector);

		logger.debug("Receiver:{} initialized ZMQ context and sockets",id);

		//start LB listener thread
		lbListener=new LbListener(topicName,context,
				lbListenerConnector,subQueueConnector,connected);
		lbListenerThread= new Thread(lbListener);
		lbListenerThread.start();

		logger.debug("Receiver:{} started topic's LB listener thread",id);

		//start connection monitoring thread
		subSocket.monitor(CONNECTION_MONITORING_LOCATOR, 
				ZMQ.EVENT_CONNECTED + ZMQ.EVENT_DISCONNECTED +
				ZMQ.EVENT_CONNECT_RETRIED + ZMQ.EVENT_MONITOR_STOPPED);
		monitoringThread= new Thread(new Monitor(context,connected));
		monitoringThread.start();
		
		logger.debug("Receiver:{} started connection monitoring thread",id);
	}

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

		if(res.startsWith("Error")){
			logger.error("Receiver:{} FE responded with error:{}",id,res);
			return null;
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
			
			return String.format("tcp://%s:%s", ebAddress, topicSendPort);
		}
	}
	
	private void  process(){
		subSocket.subscribe(topicName.getBytes());
		ZMQ.Poller poller = context.poller(2);
		poller.register(subSocket, ZMQ.Poller.POLLIN);
		poller.register(ctrlSocket, ZMQ.Poller.POLLIN);

		//poll for data and control messages
		while (!Thread.currentThread().isInterrupted() &&
				connectionState.get() == STATE_CONNECTED){
			poller.poll(POLL_INTERVAL_MILISEC);
			if (poller.pollin(0)) {//process data 
				ZMsg receivedMsg = ZMsg.recvMsg(subSocket);
				//forward received message to collector thread
				collectorSocket.send(receivedMsg.getLast().getData());
			}
			if(poller.pollin(1)){//process control message
				String command= ctrlSocket.recvStr();
				if(command.equals(Subscriber.SUBSCRIBER_EXIT_COMMAND)){
					exited.set(true);
					break;
				}
			}
		}
	}

	private void cleanup(){
		logger.info("Receiver:{} will cleanup ZMQ and close monitoring, LB threads",id);
		//close subscriber socket
		subSocket.setLinger(0);
		subSocket.close();
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
		collectorSocket.setLinger(0);
		collectorSocket.close();
		//terminate ZMQ context
		context.term();

		logger.info("Receiver:{} cleaned-up ZMQ and closed monitoring, LB threads",id);
	}

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
