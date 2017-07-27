package edu.vanderbilt.edgent.monitoring;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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
	private DistributedBarrier testFinishedBarrier;

	//Files for logging network and util stats    
	private String nwStatsFile;
	private String utilStatsFile;

	private String hostName;
	private Logger logger;

	public Monitor(String zkConnector, String runId, String outDir){
		logger=  LogManager.getLogger(this.getClass().getSimpleName());
		
		String pid= UtilMethods.pid();
		hostName=UtilMethods.hostName();

		new File(outDir+"/"+runId).mkdirs();
		utilStatsFile=String.format("%s/%s/util_%s_%s.csv",outDir,runId,hostName,pid);
		nwStatsFile=String.format("%s/%s/nw_%s_%s.csv",outDir,runId,hostName,pid);
		

		client= CuratorFrameworkFactory.newClient(zkConnector, new ExponentialBackoffRetry(1000, 3));
		client.start();
		znodePath=String.format("/experiment/%s/monitor/monitor_%s",runId,hostName);
		testStartedBarrier=new DistributedBarrier(client, String.format("/experiment/%s/barriers/pub", runId));
		testFinishedBarrier=new DistributedBarrier(client, String.format("/experiment/%s/barriers/finished", runId));

		logger.debug("Initialized monitor for host:{}",hostName);
	}
	
	public void start_monitoring(){
		try {
			//Create znode for this monitoring process
			client.create().forPath(znodePath, new byte[0]);
			logger.debug("Monitor for host:{} created znode:{}",hostName,znodePath);
			
			//wait until test starts
			logger.debug("Monitor for host:{} will wait for test to start",hostName);
			testStartedBarrier.waitOnBarrier();
		
			//record start ts
			long startTs=Instant.now().toEpochMilli();
			logger.debug("Monitor for host:{} recorded start ts:{}",hostName,startTs);
			
			//wait until test ends
			logger.debug("Monitor for host:{} will wait for test to end",hostName);
			testFinishedBarrier.waitOnBarrier();
		
			//record end ts
			long endTs=Instant.now().toEpochMilli();
			logger.debug("Monitor for host:{} recorded end ts:{}",hostName,endTs);

			//collect sysstat metrics 
			collectStats(startTs,endTs);
			
			//delete znode for this monitoring process before exiting
			client.delete().forPath(znodePath);
			
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}finally{
			CloseableUtils.closeQuietly(client);
		}
	}
	
	private void collectStats(long startTs,long endTs){
		SimpleDateFormat formatter= new SimpleDateFormat("HH:mm:ss");
		String startTime= formatter.format(new Date(startTs));
		String endTime= formatter.format(new Date(endTs));

		//collect system utilization metrics 
		String command=String.format("sadf -s %s -e %s -U -h -d -- -ur",startTime,endTime);
		executeSystemCommand(command,utilStatsFile);

		//collect network utilization metrics
		command=String.format("sadf -s %s -e %s -U -h -d -- -n DEV",startTime,endTime);
		executeSystemCommand(command,nwStatsFile);
	}

	/*
	 * Helper function to execute command and capture its output
	 */
	private void executeSystemCommand(String command,String output){
		PrintWriter writer=null;
		try {
			writer= new PrintWriter(output,"UTF-8");
			Process p = Runtime.getRuntime().exec(command);
			BufferedReader stdInp = new BufferedReader(new InputStreamReader(p.getInputStream()));
			BufferedReader stdErr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			String line;
			while ((line=stdInp.readLine()) != null) {
                writer.write(line+"\n");
            }
			while ((line=stdErr.readLine()) != null) {
				logger.error(line+"\n");
            }
			stdInp.close();
			stdErr.close();
            
		} catch (IOException e) {
			logger.error(e.getMessage(),e);
		}finally{
			writer.close();
		}
	}
	
	public static void main(String args[]){

		if(args.length < 3){
			System.out.println("Usage: Monitor outDir runId zkConnector");
			return;
		}
		String outDir=args[0];
		String runId=args[1];
		String zkConnector=args[2];
		new Monitor(zkConnector,runId,outDir).start_monitoring();
	}

}
