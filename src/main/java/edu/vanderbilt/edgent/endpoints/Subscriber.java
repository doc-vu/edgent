package edu.vanderbilt.edgent.endpoints;

import java.util.HashMap;
import org.zeromq.ZMQ;

public class Subscriber implements Runnable{
	private String topicName;
	private HashMap<Integer,Thread> receiverThreads;
	private HashMap<Integer,Receiver> receivers;
	private ZMQ.Context context;
	private ZMQ.Socket controlSocket;
	private ZMQ.Socket lbQueueSocket;

	private int receiverId;
	private String controlConnector;
	private String collectorConnector;
	private String lbQueueConnector;
	private Thread collectorThread;

	public static final String EXIT_COMMAND="exit";
	private static final int SUBSCRIBER_PORT_NUM_BASE=16006;
	private static final int COLLECTOR_PORT_NUM_BASE=9006;
	private static final int LB_QUEUE_PORT_NUM_BASE=4006; 

	public Subscriber(String topicName,int id){
		this.topicName=topicName;
		this.context=ZMQ.context(1);
		receivers=new HashMap<Integer,Receiver>();
		receiverThreads=new HashMap<Integer,Thread>();
		receiverId=0;
		controlConnector=String.format("tcp://*:%d",
				(SUBSCRIBER_PORT_NUM_BASE+id));
		collectorConnector=String.format("tcp://*:%d",
				(COLLECTOR_PORT_NUM_BASE+id));
		lbQueueConnector=String.format("tcp://*:%d",
				(LB_QUEUE_PORT_NUM_BASE+id));
		System.out.println("Subscriber initialized");
	}

	@Override
	public void run() {
		System.out.println("Subscriber started");
		lbQueueSocket= context.socket(ZMQ.PULL);
		lbQueueSocket.bind(lbQueueConnector);

		controlSocket= context.socket(ZMQ.PUB);
		controlSocket.bind(controlConnector);

		System.out.println("Subscriber will start collector thread");
		collectorThread= new Thread(new Collector(context,topicName,controlConnector,collectorConnector));
		collectorThread.start();

		receivers.put(receiverId, new Receiver(topicName,receiverId,
				controlConnector,lbQueueConnector,collectorConnector));
		receiverThreads.put(receiverId, new Thread(receivers.get(receiverId)));
		receiverId++;
		System.out.println("Subscriber will start Receiver thread");
		receiverThreads.get(0).start();

		while (!Thread.currentThread().isInterrupted()) {
			String command = lbQueueSocket.recvStr();
			System.out.println("Subscriber received message:"+ command);
			if (command.equals(EXIT_COMMAND)) {
				break;
			}
		}

		System.out.println("Subscriber will wait for Receiver threads to exit");
		controlSocket.send(String.format("%s,stop", topicName));
		try {
			for (int receiverId: receivers.keySet()) {
				Receiver receiver= receivers.get(receiverId);
				Thread receiverThread= receiverThreads.get(receiverId);
				if(receiver.connected()==Receiver.STATE_DISCONNECTED){
					receiverThread.interrupt();
				}
				receiverThread.join();
			}
			System.out.println("Subscriber: Receiver threads have exited");
			System.out.println("Subscriber will wait for collector thread to exit");
			collectorThread.join();
			System.out.println("Subscriber: collector thread has exited");
		} catch (InterruptedException ie) {
		}
		controlSocket.setLinger(0);
		lbQueueSocket.setLinger(0);

		controlSocket.close();
		lbQueueSocket.close();
		context.term();
	}
	
	public static void main(String args[]){
		if(args.length < 2){
			System.out.println("Subscriber topicName id");
			return;
		}
		try{
			String topicName = args[0];
			int id = Integer.parseInt(args[1]);
			Thread subscriber = new Thread(new Subscriber(topicName,id));

			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					try {
						ZMQ.Context context= ZMQ.context(1);
						ZMQ.Socket pushSocket= context.socket(ZMQ.PUSH);
						pushSocket.connect(String.format("tcp://*:%d",
								(Subscriber.LB_QUEUE_PORT_NUM_BASE+id)));
						pushSocket.send(Subscriber.EXIT_COMMAND);
						subscriber.join();
						System.out.println("Subscriber has exited");
						pushSocket.setLinger(0);
						pushSocket.close();
						context.term();
					} catch (InterruptedException e) {
					}
				}
			});
			subscriber.start();
		}catch(NumberFormatException e){
			e.printStackTrace();
		}
	}
}
