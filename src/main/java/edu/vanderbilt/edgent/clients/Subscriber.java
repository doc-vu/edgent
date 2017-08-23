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
public class Subscriber extends Client{
	//Curator client to connect to zk 
	private CuratorFramework client;
	private String expZnodePath;
	//File to log recorded latencies of reception
	private static String latencyFile;
	private static PrintWriter writer;
	
	private Logger logger;
	
	public Subscriber(String topicName, int regionId, 
			int runId, int sampleCount, String outDir,String zkConnector){
		super(Client.TYPE_SUB,topicName,sampleCount);
		logger= LogManager.getLogger(this.getClass().getSimpleName());

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
		expZnodePath=String.format("/experiment/%s/sub/sub_%s_%s",runId,hostName,pid);
	}
	
	
	public void receive(){
		while(connectionState.get()==Client.STATE_CONNECTED && currCount<sampleCount){
			//Blocking receive call to get data
			ZMsg receivedMsg= ZMsg.recvMsg(socket);
			currCount++;

			// Parse received data sample
			long reception_ts = System.currentTimeMillis();
			DataSample sample = DataSampleHelper.deserialize(receivedMsg.getLast().getData());

			// log recorded latency
			long latency = Math.abs(reception_ts - sample.tsMilisec());
			writer.write(String.format("%d,%d,%d,%d\n", sample.sampleId(), reception_ts, sample.tsMilisec(), latency));

			//if (currCount % 1000 == 0) {
				logger.debug("Subscriber for topic:{} received sample id:{} at reception_ts:{} latency:{}", topicName,
						sample.sampleId(), reception_ts, latency);
			//}
		}
	}
	private void experimentSetup(){
		try{
			if(client.checkExists().forPath(expZnodePath)==null){
				client.create().forPath(expZnodePath, new byte[0]);
				logger.debug("Subscriber:{} for topic:{} created znode:{}", id, topicName, expZnodePath);
			}
		}catch(Exception e){
			logger.error("Subscriber:{} for topic:{} caught exception:{}",id,topicName,e.getMessage());
		}
	}
	
	@Override
	public void process() {
		socket.subscribe(topicName.getBytes());
		ZMQ.Poller poller= context.poller(1);
		poller.register(socket,ZMQ.Poller.POLLIN);
		experimentSetup();
		while(connectionState.get()==Client.STATE_CONNECTED && currCount<sampleCount){
			poller.poll(5000);
			if(poller.pollin(0)){
				// Blocking receive call to get data
				ZMsg receivedMsg = ZMsg.recvMsg(socket);
				currCount++;

				// Parse received data sample
				long reception_ts = System.currentTimeMillis();
				DataSample sample = DataSampleHelper.deserialize(receivedMsg.getLast().getData());

				// log recorded latency
				long latency = Math.abs(reception_ts - sample.tsMilisec());
				writer.write(
						String.format("%d,%d,%d,%d\n", sample.sampleId(), reception_ts, sample.tsMilisec(), latency));

				// if (currCount % 1000 == 0) {
				logger.debug("Subscriber for topic:{} received sample id:{} at reception_ts:{} latency:{}", topicName,
						sample.sampleId(), reception_ts, latency);
				// }
			}
		}
	}

	@Override
	public void shutdown() {
		writer.close();
		System.out.println("closed zk");
		CloseableUtils.closeQuietly(client);
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
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					//stop subscriber 
					subscriber.stop();
				}
			});
			subscriber.start();
			
		}catch(NumberFormatException e){
			e.printStackTrace();
		}
	}
}
