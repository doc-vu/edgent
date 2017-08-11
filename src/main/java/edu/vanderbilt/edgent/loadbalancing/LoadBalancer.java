package edu.vanderbilt.edgent.loadbalancing;

import java.util.ArrayList;
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

/**
 * LoadBalancer is responsible for the following operations: 
 * 1. Listens for incoming topic creation requests from the FE 
 * and processes them on a pool of worker threads.
 * 2. LB thread periodically comes up to take LB decisions
 * @author kharesp
 *
 */
public class LoadBalancer {
	//ZMQ Context 
	private ZContext context;
	//ZMQ front-facing Router and backend Dealer sockets for multi-threaded operation
	private ZMQ.Socket listener;
	private ZMQ.Socket distributor;

	//Front facing Router and backend Dealer socket endpoints
	public static final int LISTENER_PORT=7777;
	public static final String INPROC_CONNECTOR="inproc://lbWorkers";

	//Number of concurrent threads servicing incoming requests
	public static final int WORKER_POOL_SIZE=5;
	private List<Thread> workers;
	
	//Curator client for connecting to ZK 
	private CuratorFramework client;

	private String lbId;
	private Logger logger;
	
	public LoadBalancer(int regionId,ZContext context,String zkConnector)
	{
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		lbId=String.format("LB-%d-%s",regionId,UtilMethods.ipAddress());

		this.context=context;
		//initialize front-facing ROUTER socket to listen to topic creation requests
		listener=context.createSocket(ZMQ.ROUTER);
		listener.bind(String.format("tcp://*:%d",LISTENER_PORT));

		//initialize backend distributer socket to distribute topic creation requests
		distributor=context.createSocket(ZMQ.DEALER);
		distributor.bind(INPROC_CONNECTOR);
	
		//initialize Curator client for ZK connection
		client=CuratorFrameworkFactory.newClient(zkConnector,
				new ExponentialBackoffRetry(1000, 3));
		client.start();

		//create  worker threads to process incoming requests concurrently
		workers= new ArrayList<Thread>();
		for(int i=0;i<WORKER_POOL_SIZE;i++){
			workers.add(new Thread(new WorkerThread(ZContext.shadow(context),
					client)));
		}

		logger.debug("Initalized LB:{}",lbId);
	}
	
	public void start(){
		//start worker threads
		logger.debug("LB:{} starting worker threads",lbId);
		for(Thread t:workers){
			t.start();
		}
		
		//start ZMQ proxy to listen for incoming requests and forward them to worker pool
		logger.debug("LB:{} will start listening for topic creation requests at port:{}",
				lbId,LISTENER_PORT);
		ZMQ.proxy(listener,distributor, null);
		
		//LB was terminated, perform clean-up
		logger.debug("LB:{} proxy was terminated",lbId);
		context.destroy();
		CloseableUtils.closeQuietly(client);
		
		logger.debug("LB:{} will wait for worker threads to exit",lbId);
		try{
			for (Thread t : workers) {
				t.interrupt();
				t.join();
			}
		}catch(InterruptedException e){}

		logger.debug("LB:{} exited",lbId);
	}
	
	public static void main(String args[]){
		if(args.length<2){
			System.out.println("Usage: LoadBalancer regionId zkConnector");
			return;
		}
		try{
			//parse commandline arguments
			int regionId=Integer.parseInt(args[0]);
			String zkConnector=args[1];

			//Create ZContext
			ZContext context=new ZContext();
			//Initialize LoadBalancer with shadowed Zcontext
			LoadBalancer lb=new LoadBalancer(regionId,
					ZContext.shadow(context),
					zkConnector);
			
			//Register listener to handle SIGINT and SIGTERM
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					context.destroy();
				}
			});

			//start LB
			lb.start();
			
		}catch(NumberFormatException e){
			e.printStackTrace();
		}
	}
}
