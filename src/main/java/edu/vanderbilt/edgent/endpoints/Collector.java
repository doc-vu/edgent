package edu.vanderbilt.edgent.endpoints;

import org.zeromq.ZMQ;
import edu.vanderbilt.edgent.types.DataSample;
import edu.vanderbilt.edgent.types.DataSampleHelper;

public class Collector implements Runnable{
	private ZMQ.Context context;
	private ZMQ.Socket collectorSocket;
	private ZMQ.Socket controlSocket;

	private String topicName;
	private String controlConnector;
	private String collectorConnector;
	
	private static final int POLL_INTERVAL_MILISEC=5000;

	public Collector(ZMQ.Context context,String topicName,
			String controlConnector,String collectorConnector){
		this.context=context;
		this.topicName=topicName;
		this.controlConnector=controlConnector;
		this.collectorConnector=collectorConnector;
	}

	@Override
	public void run() {
		System.out.println("Collector started");
		collectorSocket= context.socket(ZMQ.PULL);
		collectorSocket.bind(collectorConnector);

		controlSocket= context.socket(ZMQ.SUB);
		controlSocket.connect(controlConnector);
		controlSocket.subscribe(topicName.getBytes());

		ZMQ.Poller poller= context.poller(2);
		poller.register(collectorSocket,ZMQ.Poller.POLLIN);
		poller.register(controlSocket,ZMQ.Poller.POLLIN);
		while(!Thread.currentThread().isInterrupted()){
			poller.poll(POLL_INTERVAL_MILISEC);
			if(poller.pollin(0)){
				// Parse received data sample
				//long reception_ts = System.currentTimeMillis();
				DataSample sample = DataSampleHelper.deserialize(collectorSocket.recv());

				// log recorded latency
				//long latency = Math.abs(reception_ts - sample.tsMilisec());
				System.out.println("Collector received data sample:"+sample.sampleId());
			}
			if(poller.pollin(1)){
				String command= controlSocket.recvStr();
				System.out.println("Collector received control:"+command);
				break;
			}
		}
		collectorSocket.setLinger(0);
		controlSocket.setLinger(0);

		collectorSocket.close();
		controlSocket.close();
		System.out.println("Collector exited");
	}
}
