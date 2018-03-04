package edu.vanderbilt.edgent.monitoring;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;

public class MonitorTest {
	public static void main(String args[]) throws InterruptedException{
		long startTs= Instant.now().toEpochMilli();
		int sleep_interval_s=10;
		Thread.sleep(sleep_interval_s*1000);
		long endTs= Instant.now().toEpochMilli();
		SimpleDateFormat formatter= new SimpleDateFormat("HH:mm:ss");
		String startTime= formatter.format(new Date(startTs));
		System.out.println(startTime);
		String endTime= formatter.format(new Date(endTs));
		System.out.println(endTime);

		executeSystemCommand(new String[]{"sadf","-s",startTime,"-e",endTime,"-Uhd","--","-n","DEV"},"nw.txt");
		executeSystemCommand(new String[]{"sadf","-s",startTime,"-e",endTime,"-Uhd","--","-u"},"cpu.txt");
		executeSystemCommand(new String[]{"sadf","-s",startTime,"-e",endTime,"-Uhd","--","-r"},"mem.txt");
		
	}
	public static void executeSystemCommand(String[] command,String output){
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

}
