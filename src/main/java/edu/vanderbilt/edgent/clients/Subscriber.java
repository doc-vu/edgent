package edu.vanderbilt.edgent.clients;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
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
	
	private long startTs;
	private long endTs;

	//File to log recorded latencies of reception
	private static PrintWriter writer;
	private static String latencyFile;
	
	private AtomicBoolean interrupted;
	
	public Subscriber(String topicName, int runId, int sampleCount, 
			String outDir,String zkConnector) throws Exception{
		//initialize client endpoint by calling the super constructor
		super(Client.TYPE_SUB,topicName,sampleCount);
		
		interrupted= new AtomicBoolean(false);
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

		//listener loop will execute until all samples are not received while still connected to EB 
		while(connectionState.get()==Client.STATE_CONNECTED && currCount<sampleCount){
			logger.debug("{}:{} for topic:{} will poll for data",endpointType,id,topicName);
			poller.poll(2000);
			if (poller.pollin(0)) {
				ZMsg receivedMsg = ZMsg.recvMsg(socket);
				currCount++;
				if (currCount == 1) {
					startTs = System.currentTimeMillis();
				}

				// Parse received data sample
				long reception_ts = System.currentTimeMillis();
				DataSample sample = DataSampleHelper.deserialize(receivedMsg.getLast().getData());

				// log recorded latency
				long latency = Math.abs(reception_ts - sample.tsMilisec());
				writer.write(
						String.format("%d,%d,%d,%d\n", sample.sampleId(), reception_ts, sample.tsMilisec(), latency));

				if (currCount % 1000 == 0) {
					logger.debug("{}:{} for topic:{} received sample id:{} at reception_ts:{} latency:{}", endpointType,
							id, topicName, sample.sampleId(), reception_ts, latency);
				}
			}
		}
		endTs=System.currentTimeMillis();
		double thput=(1000.0*sampleCount)/(endTs-startTs);
		logger.info("{}:{} for topic:{} observed a throughput of {} msgs/sec",endpointType,id,topicName,thput);
	}

	@Override
	public void onExit() {
		interrupted.set(true);
		
		writer.close();
		try {
			if(client.getState()==CuratorFrameworkState.STARTED){
				client.delete().forPath(expZnodePath);
			}
		} catch (Exception e) {
			logger.error("{}:{} for topic:{} caught exception:{}",endpointType,
				id,e.getMessage());
		}
		CloseableUtils.closeQuietly(client);
		logger.info("{}:{} for topic:{} deleted its experiment znode:{} and closed zk connection",endpointType,
				id,topicName,expZnodePath);
	}
	
	@Override
	public void onConnected() {
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

	public static void main(String args[]){
		if(args.length<5){
			System.out.println("Usage: Subscriber topicName, runId, sampleCount, "
					+ "outDir, zkConnector");
			return;
		}
		try{
			//parse commandline arguments
			String topicName= args[0];
			int runId= Integer.parseInt(args[1]);
			int sampleCount= Integer.parseInt(args[2]);
			String outDir= args[3];
			String zkConnector=args[4];
		
			//initialize subscriber
			Subscriber subscriber= new Subscriber(topicName,runId,
					sampleCount,outDir,zkConnector);

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
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}

}
