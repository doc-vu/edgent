package edu.vanderbilt.edgent.clients;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import edu.vanderbilt.edgent.fe.Frontend;
import edu.vanderbilt.edgent.util.UtilMethods;

public abstract class Client {
	//topicName
	protected String topicName;
	//total number of samples to be sent/received
	protected int sampleCount;
    //current number of samples sent/received 
    protected int currCount;

	//ZMQ context and sockets
	protected ZMQ.Context context;
	protected ZMQ.Socket socket;
	private ZMQ.Socket feSocket;

	//thread for monitoring socket's connection state
    private Thread monitoringThread;
    //znode path 
    private String znodePath;
   
    //variable to track current connection state
  	protected AtomicInteger connectionState;
  	//variable to track number of retry attempts
  	private AtomicInteger retryCount;
  
  	//waitset to wait on until connected to EB
  	private CountDownLatch connected;
  
  	//
  	private String endpointType;
  	public static final String TYPE_SUB="sub";
  	public static final String TYPE_PUB="pub";

  	//Connection states
  	protected static final int STATE_CONNECTED=1;
  	protected static final int STATE_DISCONNECTED=0;
  	//Maximum number of times client will try connecting to the same EB location 
  	private static final int MAX_RETRY_COUNT=15;
  	//
  	private static final String CONNECTION_MONITORING_LOCATOR="inproc://monitor";

  	protected String id;
  	protected Logger logger;	
  	
  	public Client(String type,String topicName,int sampleCount){
  		logger= LogManager.getLogger(this.getClass().getSimpleName());

  		this.endpointType=type;
		this.topicName=topicName;
		this.sampleCount=sampleCount;

		znodePath="";
		id=String.format("%s_%s_%s",type,UtilMethods.hostName(),UtilMethods.pid());
		connectionState= new AtomicInteger(STATE_DISCONNECTED);
		retryCount= new AtomicInteger(0);

		logger.debug("{}:{} initialized for topic:{}",type,id,topicName);
  	}
  	
  	public void start() {
		try{
			while (currCount < sampleCount) {
				// try connecting to an EB
				connected = new CountDownLatch(1);
				initializeZMQ();
				String ebLocator= queryFe();
				if(ebLocator==null){
					cleanup();
					break;
				}
				socket.connect(ebLocator);
				// wait until connected to EB
				connected.await();
				if (connectionState.get() == STATE_CONNECTED) {
					// process if connected to EB
					process();
				}
				if(currCount==sampleCount){
					//request FE to delete this client's znode if all data has been sent/received
					feSocket.send(String.format("%s,%s", Frontend.DISCONNECTION_REQUEST,znodePath));
				}
				//close ZMQ sockets and context- either all data was sent/disconnected/retrying connection
				cleanup();
			}
			shutdown();
		}catch(Exception e){
			logger.error("{}:{} for topic:{} caught exception:{}",endpointType,id,topicName,e.getMessage());
		}
	}
  	
  	public void stop(){
  		System.out.println("stop called");
  		cleanup();
  		shutdown();
  	}
  	
  	private void initializeZMQ(){
		//initialize ZMQ Context 
		context=ZMQ.context(1);
		//create REQ socket to query FE
		feSocket=context.socket(ZMQ.REQ);
		feSocket.connect(String.format("tcp://127.0.0.1:%d",Frontend.LISTENER_PORT));
		//create PUB/SUB socket and associated monitor to track its connection status to EB
		if(endpointType.equals(TYPE_PUB)){
			socket= context.socket(ZMQ.PUB);
		}else if(endpointType.equals(TYPE_SUB)){
			socket= context.socket(ZMQ.SUB);
		}
		socket.monitor(CONNECTION_MONITORING_LOCATOR, 
				ZMQ.EVENT_CONNECTED + ZMQ.EVENT_DISCONNECTED +
				ZMQ.EVENT_CONNECT_RETRIED + ZMQ.EVENT_MONITOR_STOPPED);
		//start monitoring thread
		monitoringThread= new Thread(new Monitor(context,connected));
		monitoringThread.start();
		logger.debug("{}:{} for topic:{} started monitoring thread",endpointType,id,topicName);
  	}
  	
  	private String queryFe(){
		feSocket.send(String.format("%s,%s,%s,%s,%s",
				Frontend.CONNECTION_REQUEST,
				topicName,
				endpointType,UtilMethods.ipAddress(),
				znodePath));
		String res= feSocket.recvStr();
		if(!res.startsWith("Error")){
			String[] parts= res.split(";");
			logger.debug("{}:{} for topic:{} got eb location:{} and znode path:{}",
					endpointType, id, topicName, parts[0],parts[1]);
			znodePath = parts[1];
			String[] locatorParts = parts[0].split(",");
			String ebLocator = "";
			if (endpointType.equals(TYPE_PUB))
				ebLocator = String.format("tcp://%s:%s", locatorParts[0], locatorParts[1]);
			else if (endpointType.equals(TYPE_SUB)) {
				ebLocator = String.format("tcp://%s:%s", locatorParts[0], locatorParts[2]);
			}
			return ebLocator;
		}else{
			return null;
		}
	}
  	
  	private void cleanup(){
		//close socket
		socket.close();
		//wait for monitoring thread to exit
		try {
			if(monitoringThread!=null){
				monitoringThread.join();
			}
		} catch (InterruptedException e) {
			logger.error("Publisher:{} for topic:{} caught exception:{}",
					id,topicName,e.getMessage());
		}
		//close fe request socket and context
		feSocket.close();
		context.close();
		logger.debug("{}:{} for topic:{} closed ZMQ sockets and context",endpointType,id,topicName);
	}
  	
  	private class Monitor implements Runnable{
		private CountDownLatch connected;
		private ZMQ.Context context;
		public Monitor(ZMQ.Context context,CountDownLatch latch){
			this.context=context;
			connected=latch;
		}
		@Override
		public void run() {
			logger.debug("{}:{} for topic:{} monitoring thread:{} started",
					endpointType,id,topicName,Thread.currentThread().getName());
			ZMQ.Socket pair = context.socket(ZMQ.PAIR);
			pair.connect(CONNECTION_MONITORING_LOCATOR);
			while (true) {
				ZMQ.Event event = ZMQ.Event.recv(pair);
				//connected to EB
				if (event.getEvent() == ZMQ.EVENT_CONNECTED) {
					//set retry count to 0
					retryCount.set(0);
					//set connection state to CONNECTED
					connectionState.set(STATE_CONNECTED);
					//open waitset
					connected.countDown();
					logger.debug("{}:{} for topic:{} monitoring thread:{} connected to EB:{}",
							endpointType,id,topicName,Thread.currentThread().getName(),event.getAddress());
				}
				if (event.getEvent() == ZMQ.EVENT_CONNECT_RETRIED) {
					//increment retry count
					int val=retryCount.getAndIncrement();
					//set connection state to DISCONNECTED 
					connectionState.set(STATE_DISCONNECTED);
					logger.debug("{}:{} for topic:{} monitoring thread:{} #retry attempt:{} to EB:{}",
							endpointType,id,topicName,Thread.currentThread().getName(),val,event.getAddress());
	//context.destroy();
					if(val>MAX_RETRY_COUNT){
						//open waitset
						connected.countDown();
						//set retry count to 0
						retryCount.set(0);
						break;
					}
				}
				if (event.getEvent() == ZMQ.EVENT_DISCONNECTED) {
					//set retry count to 0
					retryCount.set(0);
					//set connection state to DISCONNECTED
					connectionState.set(STATE_DISCONNECTED);
					logger.debug("{}:{} for topic:{} monitoring thread:{} got disconnected to EB:{}",
							endpointType,id,topicName,Thread.currentThread().getName(),event.getAddress());
					break;
				}
				if(event.getEvent()== ZMQ.EVENT_MONITOR_STOPPED){
					logger.debug("{}:{} for topic:{} monitoring thread:{} monitor was closed",
							endpointType,id,topicName,Thread.currentThread().getName());
					break;
				}
			}
			pair.disconnect(CONNECTION_MONITORING_LOCATOR);
			pair.close();
		}
	}
  	
  	
  	public abstract void process();
  	public abstract void shutdown();
  	
}
