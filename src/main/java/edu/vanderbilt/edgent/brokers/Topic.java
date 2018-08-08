package edu.vanderbilt.edgent.brokers;

import java.util.concurrent.CountDownLatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;
import edu.vanderbilt.edgent.types.ContainerCommand;
import edu.vanderbilt.edgent.types.ContainerCommandHelper;
import edu.vanderbilt.edgent.types.DataSample;
import edu.vanderbilt.edgent.types.DataSampleHelper;
import edu.vanderbilt.edgent.types.TopicCommand;
import edu.vanderbilt.edgent.types.TopicCommandHelper;
import edu.vanderbilt.edgent.util.Commands;
import edu.vanderbilt.edgent.util.UtilMethods;
/**
 * This class models a Topic/Channel hosted at an Edge/Routing Broker. 
 * Each topic is managed by a thread which creates a ZMQ.SUB socket to 
 * receive incoming data and a ZMQ.PUB socket to send data out to subscribers.
 * @author kharesp
 */
public class Topic implements Runnable {
	//Topic name
	private String topicName;
	//ZMQ context
	private ZMQ.Context context;
	//ZMQ SUB socket to receive control messages from hosting broker
	private ZMQ.Socket controlSocket;
	//ZMQ SUB socket to receive topic data 
	private ZMQ.Socket receiveSocket;
	//ZMQ PUB socket to send topic data
	private ZMQ.Socket sendSocket;
	//ZMQ PUB socket to send LB commands for this topic
	private ZMQ.Socket lbSocket;

	//EB's socket at which topic control commands are issued 
	private String ebControlConnector;

	//poller to poll receiveSocket for data and controlSocket for control msgs
	private ZMQ.Poller poller;

	//Binding port numbers for receiveSocket,sendSocket and lbSocket for this topic
	private int receivePort;
	private int sendPort;
	private int lbPort;
	//string representation of this Topic's connector
	private String topicConnector;

	//state variables to track if topic is properly initialized and listening for incoming data
	private boolean listening;
	private CountDownLatch initialized;
	
	
	private DataSampleHelper dataSampleHelper;
	private ContainerCommandHelper containerCommandHelper;
	private int count;
	private long startTs;
	
	private int per_sample_processing_interval;

	private Logger logger;
	
	public Topic(String topicName,ZMQ.Context context, String ebControlConnector,
			int receivePort,int sendPort,int lbPort,CountDownLatch initialized,int per_sample_processing_interval){
		logger= LogManager.getLogger(this.getClass().getName());
		//stash constructor arguments
		this.topicName= topicName;
		this.context=context;
		this.ebControlConnector=ebControlConnector;
		this.receivePort=receivePort;
		this.sendPort=sendPort;
		this.lbPort=lbPort;
		this.initialized=initialized;
		this.dataSampleHelper=new DataSampleHelper();
		this.containerCommandHelper=new ContainerCommandHelper();
		
		this.per_sample_processing_interval=per_sample_processing_interval;
		
		topicConnector=String.format("%s,%d,%d,%d",UtilMethods.ipAddress(),
				receivePort,sendPort,lbPort);
		listening=false;
		count=0;

		logger.info("Topic:{} initialized for receive port:{} send port:{} and lb port:{} ",
				topicName,receivePort,sendPort,lbPort);
	}

	/**
	 * Listener loop for topic thread. 
	 * Data received on receive port is forwarded to send port until 
	 * the topic thread is stopped via control message received on controlSocket
	 */
	@Override
	public void run() {
		//instantiate ZMQ Sockets and poller
		controlSocket=context.socket(ZMQ.SUB);
		receiveSocket= context.socket(ZMQ.SUB);
		receiveSocket.setHWM(0);

		sendSocket= context.socket(ZMQ.PUB);
		sendSocket.setHWM(0);

		lbSocket= context.socket(ZMQ.PUB);
		poller=context.poller(2);

		try{
			//connect to EB's socket at which topic control messages are issued
			controlSocket.connect(ebControlConnector);
			// subscribe to receive topic control messages for this topicName
			controlSocket.subscribe(topicName.getBytes());

			// bind receiveSocket to receivePort and subscribe to receive this topic's data
			receiveSocket.bind(String.format("tcp://*:%d", receivePort));
			receiveSocket.subscribe(topicName.getBytes());
			logger.debug("Topic:{} receive socket bound to port:{} and subscribed to topic:{}", topicName, receivePort,
					topicName);

			// register receiveSocket and controlSocket with the poller
			poller.register(receiveSocket, ZMQ.Poller.POLLIN);
			poller.register(controlSocket, ZMQ.Poller.POLLIN);

			// bind sendSocket to sendPort to send topic's data
			sendSocket.bind(String.format("tcp://*:%d", sendPort));
			logger.debug("Topic:{} send socket bound to port number:{}", topicName, sendPort);

			// bind lbSocket to lbPort to send LB commands for this topic
			lbSocket.bind(String.format("tcp://*:%d", lbPort));
			logger.debug("Topic:{}  LB socket bound to port number:{}", topicName, lbPort);
		}catch(ZMQException e){
			//sendPort/receivePort/lbPort numbers are in use
			logger.error("Topic:{} caught exception:{}",topicName,e.getMessage());
			initialized.countDown();
			cleanup();
			return;
		}

		//If topic ports:receivePort,sendPort and lbPort are properly initialized, set listening to true
		listening=true;
		initialized.countDown();


		// topic thread's listener loop
		logger.info("Topic:{} thread will start listening", topicName);
		while (!Thread.currentThread().isInterrupted()) {
			try {
				// block until either controlSocket or receiveSocket has data
				poller.poll(-1);

				// in case receiveSocket has data
				if (poller.pollin(0)) {
					ZMsg receivedMsg = ZMsg.recvMsg(receiveSocket);
					//Ts of reception of data at the EB
					long ebReceiveTs=System.currentTimeMillis();
					//De-serialize the received data
					String msgTopic = new String(receivedMsg.getFirst().getData());
					byte[] msgContent = receivedMsg.getLast().getData();
					
					//Send new DataSample with ebReceiveTs set 
					DataSample sample= DataSampleHelper.deserialize(msgContent);

					//logger.debug("Topic:{} received message {}",topicName,sample.sampleId());

					
					//Either use spin or stress-ng to simulate per-sample processing 
					if(per_sample_processing_interval>0){
						//long spin_time_nanos= exponentialProcessingInterval(per_sample_processing_interval*1000000L);
						bogus(per_sample_processing_interval);
						//spin(spin_time_nanos);
					}

					sendSocket.sendMore(msgTopic);
					sendSocket.send(dataSampleHelper.serialize(sample.sampleId(), 
							sample.regionId(), sample.runId(), sample.priority(), sample.pubSendTs(),
							ebReceiveTs, sample.containerId(), sample.payloadLength()));

					//logger.debug("Topic:{} sent message {}",topicName,sample.sampleId());

					
					count++;
					if(count==1){
						startTs=System.currentTimeMillis();
					}
					if(count%20==0){
						double throughput=(count*1.0)/(System.currentTimeMillis()-startTs);
						logger.info("Topic:{} received {} messages.Throughput:{}",topicName,count,throughput);
					}
				}
				// in case controlSocket has data
				if (poller.pollin(1)) {
					ZMsg controlMsg= ZMsg.recvMsg(controlSocket);
					String topicName= new String(controlMsg.getFirst().getData());
					TopicCommand command= TopicCommandHelper.deserialize(controlMsg.getLast().getData());
					//process TOPIC_DELETE_COMMAND
					if(command.type()== Commands.TOPIC_DELETE_COMMAND){
						logger.debug("Topic:{} received TOPIC_DELETE_COMMAND",topicName);
						break;
					}
					//process TOPIC_LB_COMMAND
					else if(command.type()== Commands.TOPIC_LB_COMMAND){
						logger.debug("Topic:{} received LB control message",topicName);
						ContainerCommand containerCommand= command.containerCommand();
						lbSocket.sendMore(topicName.getBytes());
						lbSocket.send(containerCommandHelper.serialize(containerCommand));
					}else{
						logger.error("Topic:{} received invalid control command:{}",topicName,command.type());
					}
				}
			}catch(Exception e){
				logger.error("Topic:{} caught exception:{}",
						topicName,e.getMessage());
				break;
			}
		}
		
		cleanup();
		logger.info("Topic:{} deleted", topicName);
	}
	
	public void bogus(int per_sample_processing_interval) throws Exception{
		int cpu_ops=0;
		if(per_sample_processing_interval==10){
			cpu_ops = 1;
		}else if(per_sample_processing_interval==20){
			cpu_ops = 3;
		}else if (per_sample_processing_interval==30){
			cpu_ops=5;
		}else if(per_sample_processing_interval==40){
			cpu_ops = 6;
		}else if (per_sample_processing_interval == 50) {
			cpu_ops = 8;
		}			
		Process p = Runtime.getRuntime().exec(String.format("stress-ng --cpu 1 --cpu-method matrixprod --cpu-ops %d", cpu_ops));
		p.waitFor();
	}
	
	public void spin(long wait){
		long startTime= System.nanoTime();
		while ((System.nanoTime()-startTime)< wait){}
	}

	
	private void cleanup(){
		poller.close();
		//set linger to 0
		controlSocket.setLinger(0);
		receiveSocket.setLinger(0);
		sendSocket.setLinger(0);
		lbSocket.setLinger(0);
		//close sockets
		controlSocket.close();
		receiveSocket.close();
		sendSocket.close();
		lbSocket.close();
	}

	//Accessors for topic Name,receivePort, sendPort, lbPort
	public String name(){
		return topicName;
	}
	
	public int receivePort(){
		return receivePort;
	}
	
	public int sendPort(){
		return sendPort;
	}

	public int lbPort(){
		return lbPort;
	}
	
	public boolean listening(){
		return listening;
	}
	
	public String topicConnector(){
		return topicConnector;
	}
	
	private long exponentialProcessingInterval(long averageInterval){
		return (long)(averageInterval*(-Math.log(Math.random())));
	}
	
}
