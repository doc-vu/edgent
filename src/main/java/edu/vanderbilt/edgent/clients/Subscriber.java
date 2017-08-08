package edu.vanderbilt.edgent.clients;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
public class Subscriber {
	private String topicName;
	//identifier of the region to which this subscriber belongs
	@SuppressWarnings("unused")
	private int regionId;

	//Curator client to connect to zk 
	private CuratorFramework client;
	private String znodePath;

	//ZMQ context and subscriber socket
	private ZMQ.Context context;
	private ZMQ.Socket socket;

	//Number of expected samples to receive
	private int sampleCount;
	
	//File to log recorded latencies of reception
	private static String latencyFile;
	private static PrintWriter writer;
	
	private Logger logger;
	
	public Subscriber(String topicName, int regionId, 
			int runId, int sampleCount, String outDir,String zkConnector){
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		
		this.topicName= topicName;
		this.regionId= regionId;
		this.sampleCount=sampleCount;
		
		context=ZMQ.context(1);
		socket=context.socket(ZMQ.SUB);

		String hostName= UtilMethods.hostName();
		String pid= UtilMethods.pid();
		String file_name = topicName + "_" + hostName + "_" + pid + ".csv";
		new File(outDir + "/" + runId).mkdirs();
		latencyFile = outDir + "/" + runId + "/" + file_name;

		try {
			writer = new PrintWriter(latencyFile, "UTF-8");
			writer.write("sample_id,reception_ts,sender_ts,latency(ms)\n");
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			logger.error(e.getMessage(),e);
		}

		client= CuratorFrameworkFactory.newClient(zkConnector,new ExponentialBackoffRetry(1000, 3));
		client.start();
		znodePath=String.format("/experiment/%s/sub/sub_%s_%s",runId,hostName,pid);
		logger.debug("Initialized subscriber for topic:{}",topicName);
	}
	
	
	public void start(String ebLocator){
		try {
			//Connect to EB hosting the topic of interest
			socket.connect(ebLocator);
			socket.subscribe(topicName.getBytes());
			logger.debug("Subscriber for topic:{} connected to EB locator:{}", topicName, ebLocator);

			//Create znode for this subscriber under /experiment/runId/sub path
			client.create().forPath(znodePath, new byte[0]);
			logger.debug("Subscriber for topic:{} created znode at:{}", topicName, znodePath);
			
			//Receive data
			receive();
			logger.debug("Subscriber for topic:{} received received {} samples. Exiting", topicName, sampleCount);

			//Close socket and latency file before exiting
			writer.close();
			socket.close();
			context.term();
			logger.debug("Subscriber for topic:{} ZMQ SUB socket closed.", topicName);

			//Delete znode for this subscriber under /experiment/runId/sub path
			client.delete().forPath(znodePath);
			logger.debug("Subscriber for topic:{} deleted znode at:{}.", topicName,znodePath);
			
		} catch (Exception e) {
			logger.error(e.getMessage());
		}finally{
			CloseableUtils.closeQuietly(client);
		}
	}
	
	public void receive(){
		for(int i=0; i<sampleCount; i++){
			//Blocking receive call to get data
			ZMsg receivedMsg= ZMsg.recvMsg(socket);

			//Parse received data sample
			long reception_ts= System.currentTimeMillis();
			DataSample sample=DataSampleHelper.deserialize(receivedMsg.getLast().getData());

			//log recorded latency
			long latency = Math.abs(reception_ts - sample.tsMilisec());
			writer.write(String.format("%d,%d,%d,%d\n",sample.sampleId(),
					reception_ts,sample.tsMilisec(),latency));

			if(i%1000==0){
				logger.debug("Subscriber for topic:{} received sample id:{} at reception_ts:{} latency:{}",
						topicName,sample.sampleId(),reception_ts,latency);
			}
		}
	}
	
	public static void main(String args[]){
		if(args.length<6){
			System.out.println("Usage: Subscriber topicName, regionId, runId, sampleCount, outDir, zkConnector");
			return;
		}
		try{
			String topicName= args[0];
			int regionId= Integer.parseInt(args[1]);
			int runId= Integer.parseInt(args[2]);
			int sampleCount= Integer.parseInt(args[3]);
			String outDir= args[4];
			String zkConnector=args[5];
			
			Subscriber subscriber= new Subscriber(topicName,regionId,runId,sampleCount,outDir,zkConnector);
			//TODO acquire EB locator for the topic of interest
			int port=TemporaryHelper.topicReceivePort(topicName);
			subscriber.start(String.format("tcp://10.2.2.47:%d",port));
			
		}catch(NumberFormatException e){
			e.printStackTrace();
		}
	}

}
