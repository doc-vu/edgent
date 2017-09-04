package edu.vanderbilt.edgent.fe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import edu.vanderbilt.edgent.util.UtilMethods;

public class Frontend {
	//region-specific FE locations
	public static final HashMap<Integer,String> FE_LOCATIONS;
	static{
		FE_LOCATIONS= new HashMap<Integer,String>();
		FE_LOCATIONS.put(30,"10.20.30.2");
		FE_LOCATIONS.put(0,"127.0.0.1");
		FE_LOCATIONS.put(1,"127.0.1.1");
	}
	
	//FE Request Commands
	public static final String CONNECTION_REQUEST="connect";
	public static final String DISCONNECTION_REQUEST="disconnect";

	//ZMQ Context
	private ZContext context;
	//Router and Dealer sockets for supporting multi-threaded operation
	private ZMQ.Socket listener;
	private ZMQ.Socket distributor;
	//Front facing Router and backend Dealer socket endpoints
	public static final int LISTENER_PORT=4997;
	public static final String INPROC_CONNECTOR="inproc://feWorkers";
	
	//Number of concurrent threads servicing incoming requests 
	public static final int WORKER_POOL_SIZE=5;
	private List<Thread> workers;
	
	//Curator client for connecting to ZK 
	private CuratorFramework client;

	//Unique FE id
	private String ipAddress;
	private int regionId;
	private String feId;
	private Logger logger;

	public Frontend(ZContext context,
			String zkConnector,String lbAddress){

		logger=LogManager.getLogger(this.getClass().getSimpleName());
		ipAddress=UtilMethods.ipAddress();
		regionId= UtilMethods.regionId();
		feId=String.format("FE-%d-%s",regionId,ipAddress);

		//initialize front facing Router socket
		this.context= context;
		listener=context.createSocket(ZMQ.ROUTER);
		listener.bind(String.format("tcp://*:%d",LISTENER_PORT));

		//initialize backend distributer socket
		distributor=context.createSocket(ZMQ.DEALER);
		distributor.bind(INPROC_CONNECTOR);
		
		client=CuratorFrameworkFactory.newClient(zkConnector,
				new ExponentialBackoffRetry(1000, 3));
		client.start();
		//create  worker threads to process incoming requests concurrently
		workers= new ArrayList<Thread>();
		for(int i=0;i<WORKER_POOL_SIZE;i++){
			workers.add(new Thread(new FeWorkerThread(ZContext.shadow(context),
					client,lbAddress)));
		}

		logger.info("Initalized FE:{}",feId);
	}
	
	public void start(){
		//start worker threads
		logger.debug("FE:{} starting worker threads",feId);
		for(Thread t:workers){
			t.start();
		}
		
		//start ZMQ proxy to listen for incoming requests and forward them to a worker pool
		logger.info("FE:{} will start listening for incoming requests at port:{}",
				feId,LISTENER_PORT);
		ZMQ.proxy(listener,distributor, null);
		
		//FE was terminated, perform clean-up
		logger.info("FE:{} proxy was terminated",feId);

		//set linger to 0 and close sockets
		listener.setLinger(0);
		distributor.setLinger(0);
		listener.close();
		distributor.close();

		//clean-up
		context.destroy();
		logger.debug("FE:{} ZMQ context and sockets were closed",feId);
		CloseableUtils.closeQuietly(client);
		logger.debug("FE:{} ZK connection closed",feId);
		
		logger.debug("FE:{} will wait for worker threads to exit",feId);
		try{
			for (Thread t : workers) {
				t.interrupt();
				t.join();
			}
		}catch(InterruptedException e){}
		logger.debug("FE:{} worker threads exited",feId);

		logger.info("FE:{} exited cleanly",feId);
	}
	
	public static void main(String args[]){
		if(args.length<1){
			System.out.println("Usage: Frontend zkConnector");
			return;
		}
		// parse command line arguments
		String zkConnector = args[0];
		// Create ZContext
		ZContext context = new ZContext();
		// Initialize FE with shadowed ZContext
		// TODO: Currently, assuming that FE and LB are co-located
		Frontend fe = new Frontend(ZContext.shadow(context),
				zkConnector, "127.0.0.1");

		// Register callback to handle SIGINT AND SIGTERM properly
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				context.destroy();
			}
		});

		// start FE
		fe.start();
	}

}
