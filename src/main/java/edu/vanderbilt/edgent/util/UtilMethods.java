package edu.vanderbilt.edgent.util;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Utility methods to get ipAddress, hostName and process Id
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
	
	public static String pid(){
		return ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
	}
	

}
