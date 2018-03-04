package edu.vanderbilt.edgent.util;

import java.util.LinkedList;

public class PortList {
	//port at which EB sends control messages
	public static final int EB_TOPIC_CONTROL_PORT=2000;
	//port at which FE listens for incoming connection/disconnection requests
	public static final int FE_LISTENER_PORT=2000;
	//port at which LB listens for topic creation requests
	public static final int LB_LISTENER_PORT=3000;
	//Base command and queue port numbers for Subscriber container 
	public static final int SUBSCRIBER_COMMAND_BASE_PORT_NUM=4000;
	public static final int SUBSCRIBER_QUEUE_BASE_PORT_NUM=5000;
	public static final int SUBSCRIBER_COLLECTOR_BASE_PORT_NUM=10000;

	//Base command and queue port numbers for Publisher container 
	public static final int PUBLISHER_COMMAND_BASE_PORT_NUM=7000;
	public static final int PUBLISHER_QUEUE_BASE_PORT_NUM=8000;
	public static final int PUBLISHER_PRODUCER_BASE_PORT_NUM=9000;

	private static LinkedList<Integer> pool; 
	private static PortList instance=null;
	/*User port range is: 1024-49151*/
	private static int START_RANGE= 10000;

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
			pool= new LinkedList<Integer>();
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
