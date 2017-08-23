package edu.vanderbilt.edgent.clients;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import edu.vanderbilt.edgent.types.DataSample;
import edu.vanderbilt.edgent.types.DataSampleHelper;
import edu.vanderbilt.edgent.util.UtilMethods;
/**
 * Client Subscriber end-point to test the system
 * @author kharesp
 *
 */
public class Subscriber extends Client{
	//Curator client to connect to zk 
	private CuratorFramework client;
	//Subscriber's znode for this experiment's run
	private String expZnodePath;

	//File to log recorded latencies of reception
	private static PrintWriter writer;
	private static String latencyFile;
	
	public Subscriber(String topicName, int regionId, 
			int runId, int sampleCount, String outDir,String zkConnector,String feAddress){
		//initialize client endpoint by calling the super constructor
		super(Client.TYPE_SUB,topicName,sampleCount,feAddress);

		String hostName= UtilMethods.hostName();
		String pid= UtilMethods.pid();
		String file_name = topicName + "_" + hostName + "_" + pid + ".csv";
		//create this experiment's run's output directory
		new File(outDir + "/" + runId).mkdirs();
		//file in which recorded latencies of reception will be recorded
		latencyFile = outDir + "/" + runId + "/" + file_name;

		try {
			writer = new PrintWriter(latencyFile, "UTF-8");
			writer.write("sample_id,reception_ts,sender_ts,latency(ms)\n");
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			logger.error(e.getMessage(),e);
		}

		//connect to ZK
		client= CuratorFrameworkFactory.newClient(zkConnector,new ExponentialBackoffRetry(1000, 3));
		client.start();
		expZnodePath=String.format("/experiment/%s/sub/sub_%s_%s",runId,hostName,pid);
	}
	
	
	
	@Override
	public void process() {
		//subscribe to the topic
		socket.subscribe(topicName.getBytes());
		//initialize ZMQ poller for non-blocking receive 
		ZMQ.Poller poller= context.poller(1);
		poller.register(socket,ZMQ.Poller.POLLIN);

		//setup experiment
		experimentSetup();
		
		//listener loop will execute until all samples are not received while still connected to EB 
		while(connectionState.get()==Client.STATE_CONNECTED && currCount<sampleCount){
			//block for a maximum of 5 seconds for data
			poller.poll(5000);
			if(poller.pollin(0)){//on data reception
				ZMsg receivedMsg = ZMsg.recvMsg(socket);
				currCount++;

				// Parse received data sample
				long reception_ts = System.currentTimeMillis();
				DataSample sample = DataSampleHelper.deserialize(receivedMsg.getLast().getData());

				// log recorded latency
				long latency = Math.abs(reception_ts - sample.tsMilisec());
				writer.write(String.format("%d,%d,%d,%d\n",
						sample.sampleId(), reception_ts, sample.tsMilisec(), latency));

				if (currCount % 1000 == 0) {
					logger.debug("{}:{} for topic:{} received sample id:{} at reception_ts:{} latency:{}",
							endpointType,id,topicName, sample.sampleId(), reception_ts, latency);
				}
			}
		}
	}

	private void experimentSetup(){
		try{
			if(client.checkExists().forPath(expZnodePath)==null){
				client.create().forPath(expZnodePath, new byte[0]);
				logger.info("{}:{} for topic:{} created its experiment znode:{}",
						endpointType, id, topicName, expZnodePath);
			}
		}catch(Exception e){
			logger.error("{}:{} for topic:{} caught exception:{}",
					endpointType,id,topicName,e.getMessage());
		}
	}

	@Override
	public void shutdown() {
		writer.close();
		CloseableUtils.closeQuietly(client);
		logger.info("{}:{} for topic:{} closed zk connection",endpointType,
				id,topicName);
	}
	
	public static void main(String args[]){
		if(args.length<7){
			System.out.println("Usage: Subscriber topicName, regionId, runId, sampleCount, "
					+ "outDir, zkConnector, feAddress");
			return;
		}
		try{
			//parse commandline arguments
			String topicName= args[0];
			int regionId= Integer.parseInt(args[1]);
			int runId= Integer.parseInt(args[2]);
			int sampleCount= Integer.parseInt(args[3]);
			String outDir= args[4];
			String zkConnector=args[5];
			String feAddress=args[6];
		
			//initialize subscriber
			Subscriber subscriber= new Subscriber(topicName,regionId,runId,
					sampleCount,outDir,zkConnector,feAddress);

			//handle SIGTERM and SIGINT
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					//stop subscriber 
					subscriber.stop();
				}
			});
			//start subscriber
			subscriber.start();
			
		}catch(NumberFormatException e){
			e.printStackTrace();
		}
	}
}
