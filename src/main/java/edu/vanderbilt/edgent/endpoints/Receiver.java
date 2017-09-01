package edu.vanderbilt.edgent.endpoints;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import edu.vanderbilt.edgent.fe.Frontend;
import edu.vanderbilt.edgent.util.UtilMethods;

public class Receiver implements Runnable{
	private ZMQ.Context context;
	private ZMQ.Socket feSocket;
	private ZMQ.Socket lbSocket;
	private ZMQ.Socket subSocket;
	private ZMQ.Socket ctrlSocket;
	private ZMQ.Socket pushSocket;
	
	private ReconfigurationListener reconfListener;
	private Thread reconfListenerThread;
	private Thread monitoringThread;
	
	private CountDownLatch connected;
	protected AtomicInteger connectionState;
  	private AtomicInteger retryCount;
  	private AtomicBoolean exited;
  	
	private String topicName;
	private String receiverControlConnector;
	private String lbQueueConnector;
	private String collectorConnector;

	private String znodePath;
	private String ebAddress;
	@SuppressWarnings("unused")
	private String topicListenerPort;
	private String topicSendPort;
	private String topicControlPort;

	private String id;

	private static final String CONNECTION_MONITORING_LOCATOR="inproc://monitor";
  	protected static final int STATE_CONNECTED=1;
  	protected static final int STATE_DISCONNECTED=0;
  	private static final int MAX_RETRY_COUNT=15;
  	private static final int POLL_INTERVAL_MILISEC=5000;

	public Receiver(String topicName,int id,
			String receiverControlConnector,
			String lbQueueConnector,
			String collectorConnector){
		this.topicName=topicName;
		this.receiverControlConnector=receiverControlConnector;
		this.lbQueueConnector=lbQueueConnector;
		this.collectorConnector=collectorConnector;

		this.id=String.format("sub_%s_%s_%s_%d",topicName,
				UtilMethods.hostName(),UtilMethods.pid(),id);
		connectionState= new AtomicInteger(STATE_DISCONNECTED);
		retryCount= new AtomicInteger(0);
		exited= new AtomicBoolean(false);
		System.out.println("Receiver initialized");
	}

	@Override
	public void run() {
		try {
			System.out.println("Receiver started");
			while (!Thread.currentThread().isInterrupted() && !exited.get()) {
				connected = new CountDownLatch(1);
				initializeZMQ();
				String ebLocator = queryFe();
				if (ebLocator == null) {
					System.out.println("Receiver received invalid EB location");
					cleanup();
					break;
				} else {
					System.out.println("Receiver will try to connect to EB");
					subSocket.connect(ebLocator);
					connected.await();
					if (connectionState.get() == STATE_CONNECTED) {
						System.out.println("Receiver connected to EB. Will call process");
						process();
					}
					cleanup();
				}
			}
		} catch (InterruptedException e) {
			System.out.println("Receiver caught exception:" + e.getMessage());
			cleanup();
		} finally {
			onExit();
		}
	}
	
	public int connected(){
		return connectionState.get();
	}
	
	private void initializeZMQ(){
		System.out.println("Receiver initializing ZMQ");
		context=ZMQ.context(1);
		feSocket=context.socket(ZMQ.REQ);
		lbSocket=context.socket(ZMQ.PUB);
		subSocket=context.socket(ZMQ.SUB);
		ctrlSocket=context.socket(ZMQ.SUB);
		ctrlSocket.connect(receiverControlConnector);
		ctrlSocket.subscribe(topicName.getBytes());
		pushSocket=context.socket(ZMQ.PUSH);
		pushSocket.connect(collectorConnector);

		System.out.println("Receiver will start LB thread");
		String reconfControlConnector=String.format("inproc://%s", id);
		reconfListener=new ReconfigurationListener(topicName,context,
				reconfControlConnector,lbQueueConnector,connected);
		reconfListenerThread= new Thread(reconfListener);
		reconfListenerThread.start();
	
		System.out.println("Receiver will start Monitor thread");
		subSocket.monitor(CONNECTION_MONITORING_LOCATOR, 
				ZMQ.EVENT_CONNECTED + ZMQ.EVENT_DISCONNECTED +
				ZMQ.EVENT_CONNECT_RETRIED + ZMQ.EVENT_MONITOR_STOPPED);
		monitoringThread= new Thread(new Monitor(context,connected));
		monitoringThread.start();

		String feAddress=Frontend.FE_LOCATIONS.get(UtilMethods.regionId());
		feSocket.connect(String.format("tcp://%s:%d",feAddress,Frontend.LISTENER_PORT));
		lbSocket.bind(reconfControlConnector);
	}
	

	private String queryFe(){
		System.out.println("Receiver will query FE for EB location");
		feSocket.send(String.format("%s,%s,%s,%s,%s",
				Frontend.CONNECTION_REQUEST,
				topicName,
				"sub",
				UtilMethods.ipAddress(),
				znodePath));
		String res= feSocket.recvStr();
		System.out.println("Receiver: Fe responded with:"+res);

		if(!res.startsWith("Error")){
			String[] parts= res.split(";");
			znodePath = parts[1];
			String[] locatorParts = parts[0].split(",");
			ebAddress=locatorParts[0];
			topicListenerPort=locatorParts[1];
			topicSendPort=locatorParts[2];
			topicControlPort=locatorParts[3];
			
			return String.format("tcp://%s:%s", ebAddress, topicSendPort);
		}else{
			return null;
		}
	}
	
	private void  process(){
		System.out.println("Receiver Process called");
		subSocket.subscribe(topicName.getBytes());
		ZMQ.Poller poller = context.poller(2);
		poller.register(subSocket, ZMQ.Poller.POLLIN);
		poller.register(ctrlSocket, ZMQ.Poller.POLLIN);

		while (!Thread.currentThread().isInterrupted() && connectionState.get() == STATE_CONNECTED){
			poller.poll(POLL_INTERVAL_MILISEC);
			if (poller.pollin(0)) {
				ZMsg receivedMsg = ZMsg.recvMsg(subSocket);
				pushSocket.send(receivedMsg.getLast().getData());
			}
			if(poller.pollin(1)){
				String command= ctrlSocket.recvStr();
				System.out.println("Receiver received control message:"+command);
				exited.set(true);
				break;
			}
		}
	}

	private void cleanup(){
		System.out.println("Receiver cleanup called");
		subSocket.setLinger(0);
		subSocket.close();
		lbSocket.send(String.format("%s,stop", topicName));
		try{
			System.out.println("Receiver will close LB thread");
			if(connected.getCount()>0){
				reconfListenerThread.interrupt();
			}
			reconfListenerThread.join();
			System.out.println("Receiver LB thread has exited");
			System.out.println("Receiver will close Monitor thread");
			monitoringThread.join();
			System.out.println("Receiver Monitor thread has exited");
		}catch(InterruptedException e){
			
		}
		feSocket.setLinger(0);
		feSocket.close();
		lbSocket.setLinger(0);
		lbSocket.close();
		ctrlSocket.setLinger(0);
		ctrlSocket.close();
		pushSocket.setLinger(0);
		pushSocket.close();
		System.out.println("Receiver will terminate its ZMQ context");
		context.term();
		System.out.println("Receiver ZMQ context terminated");
	}

	private void onExit(){
		//feSocket.send(String.format("%s,%s", Frontend.DISCONNECTION_REQUEST, znodePath));
		System.out.println("Receiver thread has exited");
	}

	private class Monitor implements Runnable{
		private CountDownLatch connected;
		private ZMQ.Context context;
		public Monitor(ZMQ.Context context,CountDownLatch latch){
			this.context=context;
			connected=latch;
			System.out.println("Receiver Monitor initialized");
		}

		@Override
		public void run() {
			System.out.println("Receiver Monitor thread started");
			ZMQ.Socket pair = context.socket(ZMQ.PAIR);
			pair.connect(CONNECTION_MONITORING_LOCATOR);
			while (!Thread.currentThread().isInterrupted()) {
				ZMQ.Event event = ZMQ.Event.recv(pair);
				if (event.getEvent() == ZMQ.EVENT_CONNECTED) {//Connected to EB
					System.out.println("Receiver Monitor state:connected");
					//set retry count to 0
					retryCount.set(0);
					//set connection state to CONNECTED
					connectionState.set(STATE_CONNECTED);
					reconfListener.setTopicControlLocator(String.format("tcp://%s:%s",ebAddress,topicControlPort));
					//open waitset to allow client to start processing
					connected.countDown();
				}
				if (event.getEvent() == ZMQ.EVENT_CONNECT_RETRIED) {//retrying connection to EB
					System.out.println("Receiver Monitor state:retrying connection");
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
					System.out.println("Receiver Monitor state:disconnected");
					//set retry count to 0
					retryCount.set(0);
					//set connection state to DISCONNECTED
					connectionState.set(STATE_DISCONNECTED);
					break;
				}
				if(event.getEvent()== ZMQ.EVENT_MONITOR_STOPPED){//socket being monitored was closed
					System.out.println("Receiver Monitor received stopped signal");
					break;
				}
			}
			pair.disconnect(CONNECTION_MONITORING_LOCATOR);
			pair.setLinger(0);
			pair.close();
			System.out.println("Receiver Monitor thread has exited");
		}
	}
}
