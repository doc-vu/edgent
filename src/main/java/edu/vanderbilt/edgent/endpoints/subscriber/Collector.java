package edu.vanderbilt.edgent.endpoints.subscriber;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import edu.vanderbilt.edgent.types.ContainerCommandHelper;
import edu.vanderbilt.edgent.types.DataSample;
import edu.vanderbilt.edgent.types.DataSampleHelper;
import edu.vanderbilt.edgent.types.TopicConnector;
import edu.vanderbilt.edgent.types.TopicConnectorHelper;
import edu.vanderbilt.edgent.types.WorkerCommand;
import edu.vanderbilt.edgent.types.WorkerCommandHelper;
import edu.vanderbilt.edgent.util.Commands;
import edu.vanderbilt.edgent.util.UtilMethods;

public class Collector implements Runnable{
	//Time period after which the clean-up thread checks for and removes expired publishers state information
	private static final int STATE_CLEANUP_INTERVAL_SEC=120;
	//private static final int POLL_INTERVAL_MILISEC=5000;
	//ZMQ context
	private ZMQ.Context context;
	//ZMQ socket at which Collector thread will receive data
	private ZMQ.Socket collectorSocket;
	//ZMQ socket at which Collector thread will receive control commands
	private ZMQ.Socket controlSocket;
	//ZMQ socket at which Collector thread will send commands to Subscriber's container queue
	private ZMQ.Socket commandSocket;

	private String topicName;
	//Collector thread's socket connector at which it receives data 
	private String collectorConnector;
	//Subscriber container's socket connector at which control commands are issued 
	private String controlConnector;
	//Subscriber container's socket connector at which it receives control commands
	private String subQueueConnector;

	//This container's Id to which this collector thread belongs
	private String containerId;
	//Map of EbId to list of publishers for which data is being received via EbId
	private Hashtable<String,Set<String>> ebIdToPublishersMap;
	//Map for maintaining Deduplication logic per publisher
	private Hashtable<String,Deduplication> pubIdToDeduplicationMap;
	//List of Ebs for which receiver connection is being removed
	private HashMap<String,String> disconnectingReceivers;
	//Executor for scheduling publisher state information clean-up periodically
	private ScheduledExecutorService scheduler;

	//field for maintaining current number of received sample count
	private int currCount;
	//total number of samples to be received
	private int sampleCount;
	private long firstSampleReceiveTs;
	
	//Writer for logging latency metrics
	private ArrayList<String> readings;
	private PrintWriter writer;

	private ContainerCommandHelper containerCommandHelper;
	private Logger logger;
	private boolean logLatency;

	public Collector(String containerId, ZMQ.Context context,String topicName,
			String controlConnector,String subQueueConnector,String collectorConnector,
			int sampleCount, int runId, String logDir,boolean logLatency){
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		//stash constructor arguments
		this.containerId= containerId;
		this.context=context;
		this.topicName=topicName;
		this.controlConnector=controlConnector;
		this.subQueueConnector=subQueueConnector;
		this.collectorConnector=collectorConnector;
		this.sampleCount=sampleCount;
		this.logLatency=logLatency;
		this.containerCommandHelper=new ContainerCommandHelper();

		currCount=0;
		ebIdToPublishersMap= new Hashtable<String,Set<String>>();
		pubIdToDeduplicationMap= new Hashtable<String,Deduplication>();
		disconnectingReceivers= new HashMap<String,String>();
		scheduler= Executors.newScheduledThreadPool(1);

		if(logLatency){
			readings=new ArrayList<String>();
			openLatencyLogFile(runId,logDir);
		}
		logger.debug("Collector:{} initialized",containerId);
	}

	@Override
	public void run() {
		//create and bind socket endpoint at which Collector thread will receive data
		collectorSocket= context.socket(ZMQ.SUB);
		collectorSocket.setHWM(0);
		collectorSocket.bind(collectorConnector);
		collectorSocket.subscribe("".getBytes());
		
		//connect to Subscriber container's command socket to receive control messages 
		controlSocket= context.socket(ZMQ.SUB);
		controlSocket.connect(controlConnector);
		controlSocket.subscribe(topicName.getBytes());

		//connect to Subscriber container's queue socket to send control messages
		commandSocket= context.socket(ZMQ.PUSH);
		commandSocket.connect(subQueueConnector);
	
		//schedule periodic cleaning of Publisher state information
		scheduler.scheduleWithFixedDelay(new Cleanup(),STATE_CLEANUP_INTERVAL_SEC,
				STATE_CLEANUP_INTERVAL_SEC, TimeUnit.SECONDS);

		//create poller to poll for both data and control messages
		ZMQ.Poller poller= context.poller(2);
		poller.register(collectorSocket,ZMQ.Poller.POLLIN);
		poller.register(controlSocket,ZMQ.Poller.POLLIN);

		logger.info("Collector:{} will start polling for data",containerId);
		while(!Thread.currentThread().isInterrupted()){
			poller.poll(-1);
			//process Data received
			if(poller.pollin(0)){
				ZMsg msg = ZMsg.recvMsg(collectorSocket);
				if(logLatency){
					long receptionTs = System.currentTimeMillis();
					// Topic Name is the source EB ID from which data was
					// received
					String ebId = new String(msg.getFirst().getData());
					// De-serialize received data
					DataSample sample = DataSampleHelper.deserialize(msg.getLast().getData());
					processData(receptionTs, ebId, sample);
				}
				currCount++;

				if(currCount%10==0){
					double throughput=(currCount*1000.0)/(System.currentTimeMillis()-firstSampleReceiveTs);
					logger.info("Collector:{} current sample count:{}, throughput:{}",
							containerId,currCount,throughput);
				}
				if(currCount==sampleCount){
					logger.info("Collector:{} received all {} messages",
							containerId,sampleCount);
					commandSocket.send(containerCommandHelper.serialize(Commands.CONTAINER_EXIT_COMMAND));
					break;
				}
			}
			//process control message
			if(poller.pollin(1)){
				ZMsg msg= ZMsg.recvMsg(controlSocket);
				WorkerCommand command=WorkerCommandHelper.deserialize(msg.getLast().getData());
				int type= command.type();
				//process CONTAINER_EXIT_COMMAND
				if(type==Commands.CONTAINER_EXIT_COMMAND){
					logger.info("Collector:{} received CONTAINER_EXIT_COMMAND:{}",
							containerId,Commands.CONTAINER_EXIT_COMMAND);
					break;
				}
				//process WORKER_EXIT_COMMAND
				if(type==Commands.WORKER_EXIT_COMMAND){
					TopicConnector connector=command.topicConnector();
					logger.info("Collector:{} received WORKER_EXIT_COMMAND:{} for ebId:{}",
							containerId,Commands.WORKER_EXIT_COMMAND,connector.ebId());
					//add the receiver connected to ebId to the list of connections in the process of disconnection
					disconnectingReceivers.put(connector.ebId(), TopicConnectorHelper.toString(connector));
				}
				//process WORKER_EXITED_COMMAND
				if(type==Commands.WORKER_EXITED_COMMAND){
					String ebId=command.topicConnector().ebId();
					logger.info("Collector:{} received WORKER_EXITED_COMMAND:{} for ebId:{}",
							containerId,Commands.WORKER_EXITED_COMMAND,ebId);
					//once receiver thread for ebId has exited, remove its entry from ebIdToPublishers map
					Set<String> publishers= ebIdToPublishersMap.remove(ebId);
					//Reset the receivingDuplicates flag for pubIds associated with this ebId
					if(publishers!=null){
						for (String publisher : publishers) {
							if (pubIdToDeduplicationMap.contains(publisher)) {
								pubIdToDeduplicationMap.get(publisher).resetReceivingDuplicates();
							}
						}
					}
				}
			}
		}
		logger.info("Collector:{} received:{} samples", containerId,currCount);

		//close latency log file writer
		if(logLatency){
			for (String s: readings){
				writer.write(s);
			}
			writer.close();
		}
		//shutdown periodic state cleanup scheduler
		scheduler.shutdown();

		poller.close();
		//set linger to 0
		collectorSocket.setLinger(0);
		controlSocket.setLinger(0);
		commandSocket.setLinger(0);

		//close sockets
		collectorSocket.close();
		controlSocket.close();
		commandSocket.close();
		logger.info("Collector:{} has exited",containerId);
	}
	
	private void processData(long receptionTs,String ebId, DataSample sample){
		String pubId = sample.containerId();
		int sampleId = sample.sampleId();

		//check if we are receiving data from a new source Eb
		if (!ebIdToPublishersMap.containsKey(ebId)) {
			ebIdToPublishersMap.put(ebId, new HashSet<String>());
			ebIdToPublishersMap.get(ebId).add(pubId);
		} else {
			//add publisher id to source Eb's publisher list
			ebIdToPublishersMap.get(ebId).add(pubId);
		}

		//create deduplication instance for a new publisher 
		if (!pubIdToDeduplicationMap.containsKey(pubId)) {
			pubIdToDeduplicationMap.put(pubId, new Deduplication(pubId, sampleId));
			log(receptionTs, sample);
		} else {
			boolean status = pubIdToDeduplicationMap.get(pubId).update(sampleId);
			if (status) {
				log(receptionTs, sample);
			}
		}

		//Check if there are any disconnecting receivers
		for (Iterator<Entry<String, String>> disconnectingReceiversIter = disconnectingReceivers.entrySet().iterator();
				disconnectingReceiversIter.hasNext();) {
			Entry<String, String> connector = disconnectingReceiversIter.next();
			String eb= connector.getKey();

			logger.debug("Collector:{} receiver thread for EB:{} is exiting", containerId,eb);
			Set<String> publishers = ebIdToPublishersMap.get(eb);
			Iterator<String> it = publishers.iterator();
			boolean disconnect = true;
			while (it.hasNext()) {
				String publisher = it.next();
				if (pubIdToDeduplicationMap.containsKey(publisher)) {
					Deduplication publisherState = pubIdToDeduplicationMap.get(publisher);
					/* check if duplicates are being received for this publisher or if publisher has expired
					 * receiver thread will only be removed once duplicate messages are being received for each alive publisher 
					 */
					if (!(publisherState.receivingDuplicates() || publisherState.expired())) {
						disconnect = false;
						break;
					}
				} else {
					//if publisher has expired remove it from ebId's publisher list
					it.remove();
				}
			}
			//if receiver thread is safe to disconnect, send CONTAINER_SIGNAL_WORKER_EXIT_IMMEDIATELY_COMMAND to remove receiver thread 
			if (disconnect) {
				commandSocket.send(containerCommandHelper.serialize(
						Commands.CONTAINER_SIGNAL_WORKER_EXIT_IMMEDIATELY_COMMAND, containerId, eb, connector.getValue()));
				logger.debug("Collector:{} sent CONTAINER_SIGNAL_WORKER_EXIT_IMMEDIATELY:{} command for receiver connected to EB:{}",
						containerId,Commands.CONTAINER_SIGNAL_WORKER_EXIT_IMMEDIATELY_COMMAND,eb);
				disconnectingReceiversIter.remove();
			}
		}
	}

	private void log(long receptionTs,DataSample sample)
	{
		if(logLatency){
			// record latency of reception of this sample
			long latency = Math.abs(receptionTs - sample.pubSendTs());
			long latency_to_eb = Math.abs(sample.ebReceiveTs() - sample.pubSendTs());
			long latency_from_eb = Math.abs(receptionTs - sample.ebReceiveTs());
			//writer.write(
			readings.add(String.format("%d,%s,%d,%d,%d,%d,%d,%d\n", receptionTs, sample.containerId(), sample.sampleId(),
							sample.pubSendTs(), latency, sample.ebReceiveTs(), latency_to_eb, latency_from_eb));
		}
		// increment count of total number of samples received
		//currCount++;
		if(currCount==1){
			firstSampleReceiveTs=System.currentTimeMillis();
		}
	}

	private void openLatencyLogFile(int runId,String logDir){
		String hostName= UtilMethods.hostName();
		String pid= UtilMethods.pid();
		//latency log file name 
		String fileName = String.format("%s_%s_%s.csv",topicName,hostName,pid);

		//create log directory for this experiment's run
		new File(logDir + "/" + String.valueOf(runId)).mkdirs();

		String latencyFile = String.format("%s/%d/%s",logDir,runId,fileName);
		try{
			writer = new PrintWriter(latencyFile, "UTF-8");
			writer.write("reception_ts,container_id,sample_id,sender_ts,latency(ms),eb_receive_ts,latency_to_eb(ms),latency_from_eb(ms)\n");
		}catch(Exception e){
			logger.error("Collector:{} caught exception:{}",containerId,e.getMessage());
		}
	}
	
	private class Cleanup implements Runnable{
		@Override
		public void run() {
			logger.info("Collector:{} Cleanup scheduled", containerId);
			//iterate over all publishers 
			Iterator<Entry<String,Deduplication>> iter= pubIdToDeduplicationMap.entrySet().iterator();
			while(iter.hasNext()){
				Entry<String,Deduplication> pair= iter.next();
				//remove publisher state information if it has expired
				if(pair.getValue().expired()){
					iter.remove();
				}
			}
		}
	}
}