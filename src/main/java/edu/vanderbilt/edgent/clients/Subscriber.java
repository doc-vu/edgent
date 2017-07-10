package edu.vanderbilt.edgent.clients;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import edu.vanderbilt.edgent.types.DataSample;
import edu.vanderbilt.edgent.types.DataSampleHelper;
import edu.vanderbilt.edgent.util.UtilMethods;

public class Subscriber {
	private String topicName;
	private int regionId;
	
	private ZMQ.Context context;
	private ZMQ.Socket socket;

	private int sampleCount;
	private static String latencyFile;
	private static PrintWriter writer;
	
	private Logger logger;
	
	public Subscriber(String topicName, int regionId, 
			int runId, int sampleCount, String outDir){
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
		logger.debug("Initialized subscriber for topic:{}",topicName);

	}
	
	public void receive(String ebLocator){
		socket.connect(ebLocator);
		socket.subscribe(topicName.getBytes());

		logger.debug("Subscriber for topic:{} connected to EB locator:{}",
				topicName,ebLocator);

		for(int i=0; i<sampleCount; i++){
			ZMsg receivedMsg= ZMsg.recvMsg(socket);
			long reception_ts= System.currentTimeMillis();
			DataSample sample=DataSampleHelper.deserialize(receivedMsg.getLast().getData());
			long latency = Math.abs(reception_ts - sample.tsMilisec());

			writer.write(String.format("%d,%d,%d,%d\n",sample.sampleId(),
					reception_ts,sample.tsMilisec(),latency));
			//if(i%1000==0){
				logger.debug("Subscriber for topic:{} received sample id:{} at reception_ts:{} latency:{}",
						topicName,sample.sampleId(),reception_ts,latency);
			//}
		}
	    logger.debug("Subscriber for topic:{} received received {} samples. Exiting",
						topicName,sampleCount);
		writer.close();
		socket.close();
		context.term();
	}
	
	public static void main(String args[]){
		if(args.length<5){
			System.out.println("Usage: Subscriber topicName, regionId, runId, sampleCount, outDir");
			return;
		}
		try{
			String topicName= args[0];
			int regionId= Integer.parseInt(args[1]);
			int runId= Integer.parseInt(args[2]);
			int sampleCount= Integer.parseInt(args[3]);
			String outDir= args[4];
			
			Subscriber subscriber= new Subscriber(topicName,regionId,runId,sampleCount,outDir);
			//TODO acquire EB locator for the topic of interest
			subscriber.receive("tcp://10.2.2.47:5001");
			
		}catch(NumberFormatException e){
			e.printStackTrace();
		}
	}

}
