package edu.vanderbilt.edgent.util;

import java.util.concurrent.LinkedBlockingQueue;

public class PortList {
	public static final int FE_LISTENER_PORT=4000;
	public static final int LB_LISTENER_PORT=5000;
	/* LinkedBlockingQueue offers a synchronized implementation of 
	 * the queue interface. Topic may be created by one thread, thereby
	 * acquiring ports from the PortList on that thread,
	 * while another thread deletes and releases the ports
	 * to the PortList on this thread. 
	 */
	private static LinkedBlockingQueue<Integer> pool; 
	private static PortList instance=null;
	/*User port range is: 1024-49151*/
	private static int START_RANGE= 20000;
	//private static int END_RANGE= 20000; 

	/**
	 * Singleton instance 
	 */
	private PortList(){}


	/**
	 * Method to access the singleton instance of PortList 
	 * @return singleton instance of PortList
	 */
	public static PortList getInstance(int id){
		if(instance==null){
			instance= new PortList();
			pool= new LinkedBlockingQueue<Integer>();
			for(int i=START_RANGE+id*1000;i<(START_RANGE+id*1000 + 1000);i++){
				pool.offer(i);
			}
		}
		return instance;
	}

	/**
	 * Acquires an unused port number or null if PortList is empty
	 * @return unused port number or null if PortList is empty
	 */
	public Integer acquire(){
		return pool.poll();
	}
	
	/**
	 * Returns a free port number to be reused.
	 * @return returns true if the operation was successful else false 
	 */
	public boolean release(Integer port){
		return pool.offer(port);
	}

}
