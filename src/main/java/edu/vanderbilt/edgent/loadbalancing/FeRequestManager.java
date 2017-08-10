package edu.vanderbilt.edgent.loadbalancing;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

/**
 * FeRequestManager listens for incoming topic creation requests from the FE 
 * and processes them on a pool of worker threads.
 * @author kharesp
 *
 */
public class FeRequestManager {
	//ZMQ Context and sockets 
	private ZContext context;
	//ZMQ Router and Dealer sockets for multi-threaded operation
	private ZMQ.Socket listener;
	private ZMQ.Socket distributor;

	//Front facing Router and backend Dealer socket endpoints
	public static final int LISTENER_PORT=7777;
	public static final String IPROC_CONNECTOR="inproc://workers";

	//Number of concurrent threads servicing incoming requests
	public static final int WORKER_POOL_SIZE=5;
	private List<Thread> workers;
	
	//Curator client for connecting to ZK 
	private CuratorFramework client;

	private Logger logger;
	
	public FeRequestManager(String zkConnector)
	{
		logger= LogManager.getLogger(this.getClass().getSimpleName());
	}
	
	public static void main(String args[]){

		
	}
	

}
