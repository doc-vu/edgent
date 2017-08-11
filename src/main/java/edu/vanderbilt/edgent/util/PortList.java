package edu.vanderbilt.edgent.util;

import java.util.LinkedList;
import java.util.Queue;

public class PortList {
	private static Queue<Integer> pool; 
	private static PortList instance=null;
	//User port range: 1024-49151 
	private static int START_RANGE= 5000;
	private static int END_RANGE= 25000; 

	/**
	 * Singleton instance 
	 */
	private PortList(){}


	/**
	 * Method to access the singleton instance of PortList 
	 * @return singleton instance of PortList
	 */
	public static PortList getInstance(){
		if(instance==null){
			instance= new PortList();
			pool= new LinkedList<Integer>();
			for(int i=START_RANGE;i<END_RANGE;i++){
				pool.offer(i);
			}
		}
		return instance;
	}

	/**
	 * Acquires an unused port number 
	 * @return unused port number
	 */
	public Integer acquire(){
		return pool.poll();
	}
	
	/**
	 * Returns a free port number to be reused. 
	 */
	public void release(Integer port){
		pool.offer(port);
	}

}
