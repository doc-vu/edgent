package edu.vanderbilt.edgent.clients;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import edu.vanderbilt.edgent.fe.Frontend;
import edu.vanderbilt.edgent.util.UtilMethods;

public abstract class Client {
  	//type of endpoint
  	protected String endpointType;
	//topicName
	protected String topicName;
	//total number of samples to be sent/received
	protected int sampleCount;
    //current number of samples sent/received 
    protected int currCount;
    //znode path of this client
    private String znodePath;
    //address of FE
    private String feAddress;

	//ZMQ context and sockets
	protected ZMQ.Context context;
	protected ZMQ.Socket socket;
	private ZMQ.Socket feSocket;

	//thread for monitoring socket's connection state
    private Thread monitoringThread;
    //variable to track current connection state
  	protected AtomicInteger connectionState;
  	//variable to track number of retry attempts
  	private AtomicInteger retryCount;
  	//waitset to wait on until connected to EB
  	private CountDownLatch connected;
  
  	//endpoint types
  	public static final String TYPE_SUB="sub";
  	public static final String TYPE_PUB="pub";

  	//Connection states
  	protected static final int STATE_CONNECTED=1;
  	protected static final int STATE_DISCONNECTED=0;
  	//Maximum number of times client will try connecting to the same EB location 
  	private static final int MAX_RETRY_COUNT=15;
  	//Inproc endpoint at which client monitoring data will be sent
  	private static final String CONNECTION_MONITORING_LOCATOR="inproc://monitor";

  	//unique id of client
  	protected String id;
  	protected Logger logger;	
  	
  	public Client(String endpointType,String topicName,int sampleCount,String feAddress){
  		logger= LogManager.getLogger(this.getClass().getSimpleName());

  		this.endpointType=endpointType;
		this.topicName=topicName;
		this.sampleCount=sampleCount;
		this.feAddress=feAddress;

		znodePath="";
		id=String.format("%s_%s_%s",endpointType,UtilMethods.hostName(),UtilMethods.pid());
		//initial state is disconnected from EB
		connectionState= new AtomicInteger(STATE_DISCONNECTED);
		//intial retry attempt count is 0
		retryCount= new AtomicInteger(0);

		logger.info("{}:{} initialized for topic:{}",endpointType,id,topicName);
  	}
  	
  	public void start() {
		try{
			//loop until all sampleCount number of samples are sent
			while (currCount < sampleCount) {
				//attempt connecting to an EB
				connected = new CountDownLatch(1);
				initializeZMQ();
				logger.info("{}:{} for topic:{} will query FE for hosting EB's location",
						endpointType,id,topicName);
				//query FE to get hosting EB's address
				String ebLocator= queryFe();

				if(ebLocator==null){//EB is not found
					logger.error("{}:{} for topic:{} FE could not find a hosting EB",
							endpointType,id,topicName);
					cleanup();
					break;
				}else{//EB is found
					logger.info("{}:{} for topic:{} FE responded with EB location:{}."
							+ " Will attempt connecting to EB",
							endpointType, id, topicName,ebLocator);
					socket.connect(ebLocator);
					// wait until connected to EB
					connected.await();
					if (connectionState.get() == STATE_CONNECTED) {
						// process if connected to EB
						logger.info("{}:{} for topic:{} connected to EB:{}. Will start processing",
								endpointType,id,topicName,ebLocator);
						process();
					}
					if (currCount == sampleCount) {
						// request FE to delete this client's znode if all data has been sent/received
						logger.info("{}:{} for topic:{} sent/received all messages."
								+ "Will delete this client's znode:{}",
								endpointType,id,topicName,znodePath);
						feSocket.send(String.format("%s,%s", Frontend.DISCONNECTION_REQUEST, znodePath));
					}
					// close ZMQ sockets and context- either all data was sent or client got disconnected
					cleanup();
				}
			}
			logger.info("{}:{} for topic:{} exited. Will call shutdown code",endpointType,id,topicName);
			shutdown();
		}catch(Exception e){
			logger.error("{}:{} for topic:{} caught exception:{}",
					endpointType,id,topicName,e.getMessage());
		}
	}
  	
  	public void stop(){
  		cleanup();
  		shutdown();
  	}
  	
  	private void initializeZMQ(){
		//initialize ZMQ Context 
		context=ZMQ.context(1);
		//create REQ socket to query FE
		feSocket=context.socket(ZMQ.REQ);
		feSocket.connect(String.format("tcp://%s:%d",feAddress,Frontend.LISTENER_PORT));
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
		logger.debug("{}:{} for topic:{} initialized ZMQ context, sockets and connection monitoring thread",
				endpointType,id,topicName);
  	}
  	
  	private String queryFe(){
  		//Query FE to find out hosting EB's address
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
			logger.error("{}:{} for topic:{} FE returned error message:{}",
					endpointType,id,topicName,res);
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
			logger.error("{}:{} for topic:{} caught exception:{}",
					endpointType,id,topicName,e.getMessage());
		}
		//close fe request socket and context
		feSocket.close();
		context.close();
		logger.debug("{}:{} for topic:{} closed ZMQ sockets,context and connection monitoring thread",
				endpointType,id,topicName);
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
			logger.info("{}:{} for topic:{} monitoring thread:{} started",
					endpointType,id,topicName,Thread.currentThread().getName());
			ZMQ.Socket pair = context.socket(ZMQ.PAIR);
			pair.connect(CONNECTION_MONITORING_LOCATOR);
			while (true) {
				ZMQ.Event event = ZMQ.Event.recv(pair);
				if (event.getEvent() == ZMQ.EVENT_CONNECTED) {//Connected to EB
					//set retry count to 0
					retryCount.set(0);
					//set connection state to CONNECTED
					connectionState.set(STATE_CONNECTED);
					//open waitset to allow client to start processing
					connected.countDown();
					logger.info("{}:{} for topic:{} monitoring thread:{} connected to EB:{}",
							endpointType,id,topicName,Thread.currentThread().getName(),event.getAddress());
				}
				if (event.getEvent() == ZMQ.EVENT_CONNECT_RETRIED) {//retrying connection to EB
					//increment retry count
					int val=retryCount.getAndIncrement();
					//set connection state to DISCONNECTED 
					connectionState.set(STATE_DISCONNECTED);
					logger.info("{}:{} for topic:{} monitoring thread:{} #retry attempt:{} to EB:{}",
							endpointType,id,topicName,Thread.currentThread().getName(),val,event.getAddress());
					if(val>MAX_RETRY_COUNT){
						//set retry count to 0
						retryCount.set(0);
						//open waitset to allow client to proceed. Client will query FE to connect to another EB
						connected.countDown();
						break;
					}
				}
				if (event.getEvent() == ZMQ.EVENT_DISCONNECTED) {//disconnected from EB
					//set retry count to 0
					retryCount.set(0);
					//set connection state to DISCONNECTED
					connectionState.set(STATE_DISCONNECTED);
					logger.error("{}:{} for topic:{} monitoring thread:{} got disconnected to EB:{}",
							endpointType,id,topicName,Thread.currentThread().getName(),event.getAddress());
					break;
				}
				if(event.getEvent()== ZMQ.EVENT_MONITOR_STOPPED){//socket being monitored was closed
					logger.info("{}:{} for topic:{} monitoring thread:{} monitor was closed",
							endpointType,id,topicName,Thread.currentThread().getName());
					break;
				}
			}
			pair.disconnect(CONNECTION_MONITORING_LOCATOR);
			pair.close();
			logger.debug("{}:{} for topic:{} monitoring thread:{} exited",
					endpointType,id,topicName,Thread.currentThread().getName());
		}
	}
  	
  
  	//processing logic carried out by client when connected
  	public abstract void process();
  	//implementation specific cleanup performed before client exits
  	public abstract void shutdown();
  	
}
