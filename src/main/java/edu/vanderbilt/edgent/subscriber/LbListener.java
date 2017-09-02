package edu.vanderbilt.edgent.subscriber;

import java.util.concurrent.CountDownLatch;
import org.zeromq.ZMQ;

public class LbListener implements Runnable{
	private String topicName;
	private ZMQ.Context context;
	private ZMQ.Socket listenerSocket;
	private ZMQ.Socket controlSocket;
	private ZMQ.Socket pushSocket;

	private String controlConnector;
	private String topicControlLocator;
	private String lbQueueConnector;
	private CountDownLatch connected;
	
	public LbListener(String topicName,ZMQ.Context context,
			String controlConnector,String lbQueueConnector,CountDownLatch connected){
		this.topicName=topicName;
		this.context=context;
		this.controlConnector=controlConnector;
		this.lbQueueConnector=lbQueueConnector;
		topicControlLocator=null;
		this.connected=connected;
		System.out.println("Receiver LB thread initilized");
	}

	@Override
	public void run() {
		System.out.println("Receiver LB thread started");
		listenerSocket=context.socket(ZMQ.SUB);
		controlSocket=context.socket(ZMQ.SUB);
		controlSocket.connect(controlConnector);
		controlSocket.subscribe(topicName.getBytes());
		pushSocket=context.socket(ZMQ.PUSH);
		pushSocket.connect(lbQueueConnector);
		try {
			System.out.println("Receiver LB thread will wait until connected to EB");
			connected.await();
		}catch(InterruptedException e){
			cleanup();
			System.out.println("Receiver LB thread has exited");
			return;
		}
		if(topicControlLocator!=null){
			listenerSocket.connect(topicControlLocator);
			listenerSocket.subscribe(topicName.getBytes());
			ZMQ.Poller poller = context.poller(2);
			poller.register(listenerSocket, ZMQ.Poller.POLLIN);
			poller.register(controlSocket, ZMQ.Poller.POLLIN);

			while (true) {
				try {
					poller.poll(-1);
					if (poller.pollin(0)) {
						String reconfCommand = listenerSocket.recvStr();
						System.out.println("Receiver LB thread got LB command:" + reconfCommand);
						pushSocket.send(reconfCommand);
					}
					if (poller.pollin(1)) {
						String command = controlSocket.recvStr();
						System.out.println("Receiver LB thread got control command:" + command);
						break;
					}
				} catch (Exception e) {
					break;
				}
			}
		}
		cleanup();
		System.out.println("Receiver LB thread has exited");
	}
	
	public void setTopicControlLocator(String topicControlLocator){
		this.topicControlLocator=topicControlLocator;
	}
	
	private void cleanup(){
		System.out.println("Receiver LB thread cleanup called");
		//set linger to 0
		listenerSocket.setLinger(0);
		controlSocket.setLinger(0);
		pushSocket.setLinger(0);
		//close sockets
		listenerSocket.close();
		controlSocket.close();
		pushSocket.close();
		System.out.println("Receiver LB thread closed all sockets");
	}
}
