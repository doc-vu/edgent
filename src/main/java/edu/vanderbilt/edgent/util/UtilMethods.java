package edu.vanderbilt.edgent.util;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Utility methods to get ipAddress, hostName, region Id and process Id
 * @author kharesp
 *
 */
public class UtilMethods {
	private static Logger logger= LogManager.getLogger(UtilMethods.class.getName());

	public static String ipAddress(){
		String ip=null;
		try {
             ip= InetAddress.getLocalHost().getHostAddress();
        } catch (java.net.UnknownHostException e) {
        	logger.error(e.getMessage(),e);
        }
		return ip;
	}
	
	public static String hostName(){
		String hostname=null;
		try {
			hostname= InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
        	logger.error(e.getMessage(),e);
		}
		return hostname;
	}
	
	public static int regionId(){
		int regionId=-1;
		try{
			regionId= Integer.parseInt(ipAddress().split("\\.")[2]);
		}catch(NumberFormatException e){
        	logger.error(e.getMessage(),e);
		}
		return regionId;
	}
	
	public static String pid(){
		return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
	}
	
	public static final HashMap<String,String> hostname_alias=new HashMap<String,String>();
	static{
		hostname_alias.put("isislab30", "node17");
		hostname_alias.put("isislab25", "node18");
		hostname_alias.put("isislab24", "node19");
		hostname_alias.put("isislab23", "node20");
		hostname_alias.put("isislab22", "node21");
		hostname_alias.put("isislab21", "node22");
		hostname_alias.put("isislab26", "node23");
		hostname_alias.put("isislab27", "node24");
		hostname_alias.put("isislab28", "node25");
		hostname_alias.put("isislab29", "node26");
		hostname_alias.put("isislab19", "node27");
		hostname_alias.put("isislab17", "node28");
		hostname_alias.put("isislab15", "node29");
		hostname_alias.put("isislab14", "node30");
		hostname_alias.put("isislab13", "node31");
		hostname_alias.put("isislab12", "node32");
		hostname_alias.put("isislab11", "node33");
		hostname_alias.put("isislab10", "node34");
		hostname_alias.put("isislab9", "node35");
		hostname_alias.put("isislab8", "node36");
		hostname_alias.put("isislab7", "node37");
		hostname_alias.put("isislab6", "node38");
		hostname_alias.put("isislab5", "node39");
		hostname_alias.put("isislab2", "node40");
		hostname_alias.put("isislab1", "node41");
	}
	

}
