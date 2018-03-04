package edu.vanderbilt.edgent.monitoring;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import edu.vanderbilt.edgent.util.UtilMethods;

public class Monitor {
	//Curator client to connect to zk
	private CuratorFramework client;
	private String znodePath;
	private DistributedBarrier testStartedBarrier;
	private DistributedBarrier dataSentBarrier;
	private DistributedBarrier testFinishedBarrier;

	//Files for logging network and util stats    
	private String nwStatsFile;
	private String cpuStatsFile;
	private String memStatsFile;

	private String hostName;
	private boolean pub_node;
	private int coreCount;
	private Logger logger;

	public Monitor(String zkConnector, String runId, String outDir,boolean pub_node,String experimentType,int coreCount){
		logger=  LogManager.getLogger(this.getClass().getSimpleName());
		
		String pid= UtilMethods.pid();
		hostName=UtilMethods.hostName();

		new File(outDir+"/"+runId).mkdirs();
		cpuStatsFile=String.format("%s/%s/cpu_%s_%s.csv",outDir,runId,hostName,pid);
		memStatsFile=String.format("%s/%s/mem_%s_%s.csv",outDir,runId,hostName,pid);
		nwStatsFile=String.format("%s/%s/nw_%s_%s.csv",outDir,runId,hostName,pid);
		
		this.pub_node=pub_node;
		this.coreCount=coreCount;

		client= CuratorFrameworkFactory.newClient(zkConnector, new ExponentialBackoffRetry(1000, 3));
		client.start();
		znodePath=String.format("/experiment/%s/monitor/monitor_%s",experimentType,hostName);
		testStartedBarrier=new DistributedBarrier(client, String.format("/experiment/%s/barriers/pub",experimentType));
		dataSentBarrier=new DistributedBarrier(client,String.format("/experiment/%s/barriers/sent",experimentType));
		testFinishedBarrier=new DistributedBarrier(client,String.format("/experiment/%s/barriers/finished",experimentType));

		logger.debug("Initialized monitor for host:{}",hostName);
	}
	
	public void start_monitoring(){
		try {
			SimpleDateFormat formatter= new SimpleDateFormat("HH:mm:ss");
			//Create znode for this monitoring process
			client.create().forPath(znodePath, new byte[0]);
			logger.debug("Monitor for host:{} created znode:{}",hostName,znodePath);
			
			//wait until test starts
			logger.debug("Monitor for host:{} will wait for test to start",hostName);
			testStartedBarrier.waitOnBarrier();
		
			//record start ts
			long startTs=Instant.now().toEpochMilli();
			String startTime= formatter.format(new Date(startTs));
			logger.debug("Monitor for host:{} recorded start ts:{}",hostName,startTime);
			
			//wait until test ends
			if(pub_node){
				logger.debug("Monitor for host:{} will until all publishers have finished sending data",hostName);
				dataSentBarrier.waitOnBarrier();
			}else{
				logger.debug("Monitor for host:{} will wait for test to end",hostName);
				testFinishedBarrier.waitOnBarrier();
			}
		
			//record end ts
			long endTs=Instant.now().toEpochMilli();
			String endTime= formatter.format(new Date(endTs));
			logger.debug("Monitor for host:{} recorded end ts:{}",hostName,endTime);

			//collect sysstat metrics 
			collectStats(startTime,endTime);
			logger.debug("Monitor for host:{} collected monitoring stats",hostName);
			
			//delete znode for this monitoring process before exiting
			client.delete().forPath(znodePath);
			
			logger.debug("Monitor for host:{} exited",hostName);
			
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}finally{
			CloseableUtils.closeQuietly(client);
		}
	}
	
	private void collectStats(String startTime,String endTime){
		//collect cpu utilization data 
		if(coreCount==-1){
			executeSystemCommand(new String[]{"sadf","-s",startTime,"-e",endTime,"-Uhd","--","-u"},cpuStatsFile);
		}else{
			String cores="";
			for(int i=0;i<coreCount;i++){
				cores=cores+String.format("%d,",i);
			}
			//remove trailing comma
			cores=cores.replaceAll(",$", "");
			executeSystemCommand(new String[]{"sadf","-s",startTime,"-e",endTime,"-Uhd","--","-P",cores,"-u"},cpuStatsFile);
		}

		//collect memory utilization data 
		executeSystemCommand(new String[]{"sadf","-s",startTime,"-e",endTime,"-Uhd","--","-r"},memStatsFile);

		//collect network utilization metrics
		executeSystemCommand(new String[]{"sadf","-s",startTime,"-e",endTime,"-Uhd","--","-n","DEV"},nwStatsFile);
	}

	private void executeSystemCommand(String[] command,String output){
		try {
			ProcessBuilder builder= new ProcessBuilder(command);
			Process process= builder.redirectOutput(new File(output)).start();
			process.waitFor();
			BufferedReader stdErr = new BufferedReader(new InputStreamReader(process.getErrorStream()));
			String line;
			while ((line=stdErr.readLine()) != null) {
				System.out.print(line+"\n");
            }
			stdErr.close();
            
		} catch (IOException | InterruptedException  e) {
			e.printStackTrace();
		}
	}

	public static void main(String args[]){

		if(args.length < 6){
			System.out.println("Usage: Monitor outDir runId zkConnector pub_node experiment_type core_count");
			return;
		}
		try{
			String outDir = args[0];
			String runId = args[1];
			String zkConnector = args[2];
			int pub_node = Integer.parseInt(args[3]);
			String experimentType=args[4];
			int coreCount=Integer.parseInt(args[5]);

			if (pub_node > 0) {
				new Monitor(zkConnector, runId, outDir, true,experimentType,coreCount).start_monitoring();
			} else {
				new Monitor(zkConnector, runId, outDir, false,experimentType,coreCount).start_monitoring();
			}
		}catch(NumberFormatException e){
			e.printStackTrace();
		}
	}
}
