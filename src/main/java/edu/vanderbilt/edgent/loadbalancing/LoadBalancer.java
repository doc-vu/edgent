package edu.vanderbilt.edgent.loadbalancing;

import java.util.ArrayList;
import java.util.List;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import edu.vanderbilt.edgent.util.UtilMethods;
/**
 * LoadBalancer is responsible for the following operations: 
 * 1. Listens for incoming topic creation requests from the FE 
 * and processes them on a pool of worker threads.
 * 2. TODO: LB thread periodically comes up to take LB decisions
 * @author kharesp
 *
 */
public class LoadBalancer implements Runnable{
	//ZMQ.Context
	private ZMQ.Context context;
	//ZMQ PULL socket to listen for incoming topic creation requests
	private ZMQ.Socket listener;
	//ZMQ PUSH socket to distribute processing of topic creation requests 
	private ZMQ.Socket distributor;
	//ZMQ PUB socket to issue control messages to worker threads 
	private ZMQ.Socket controlSocket;

	//Front-Facing port at which topic creation requests are received 
	public static final int LISTENER_PORT=8741;
	//Internal Port for issuing control messages 
	public static final int CONTROL_PORT=6955;
	//Internal inproc connector to which pool threads connect to receive requests
	public static final String INPROC_CONNECTOR="inproc://lbWorkers";
	//Topic Name on which control messages will be issued by LB 
	public static final String CONTROL_TOPIC="lbControl";
	//Control messages
	public static final String SHUTDOWN_CONTROL_MSG="shutdown";

	//Worker pool size
	public static final int WORKER_POOL_SIZE=5;
	//Worker threads
	private List<Thread> workers;

	//CuratorClient to talk to ZK
	private CuratorFramework client;

	//Unique load balancer id
	private String ipAddress;
	private int regionId;
	private String lbId;
	private Logger logger;
	
	public LoadBalancer(String zkConnector) 
	{
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		ipAddress=UtilMethods.ipAddress();
		regionId= UtilMethods.regionId();
		lbId=String.format("LB-%d-%s",regionId,ipAddress);

		//Initialize ZMQ Context
		context=ZMQ.context(1);

		//Initialize CuratorClient for ZK connection
		client=CuratorFrameworkFactory.newClient(zkConnector,
				new ExponentialBackoffRetry(1000, 3));
		client.start();

		//Create worker threads
		workers= new ArrayList<Thread>();
		for(int i=0;i<WORKER_POOL_SIZE;i++){
			workers.add(new Thread(new LbWorkerThread(context,
					client)));
		}

		logger.info("Initalized LB:{}",lbId);
	}

	@Override 
	public void run(){
		//Initialize all ZMQ sockets
		listener=context.socket(ZMQ.PULL);
		listener.bind(String.format("tcp://*:%d",LISTENER_PORT));

		distributor=context.socket(ZMQ.PUSH);
		distributor.bind(INPROC_CONNECTOR);
		
		controlSocket=context.socket(ZMQ.PUB);
		controlSocket.bind(String.format("tcp://*:%d",CONTROL_PORT));
		//Start Lb Worker threads
		for(Thread t:workers){
			t.start();
		}
		logger.debug("LB:{} started worker threads",lbId);

		logger.info("LB:{} will start listening for topic creation requests at:{}",
				lbId,LISTENER_PORT);
		//Listener loop: forwards topic creation requests to worker pool
		while(true){
			String data = listener.recvStr(); 
			System.out.println(data);
			//Exit listener loop if SHUTDOWN_CONTROL_MSG is received
			if (data.equals(SHUTDOWN_CONTROL_MSG)){
				logger.info("LB:{} received {} signal.",lbId,SHUTDOWN_CONTROL_MSG);
				//send shutdown_control_msg to all worker threads
				controlSocket.send(String.format("%s %s",
						CONTROL_TOPIC,SHUTDOWN_CONTROL_MSG));
				break;
			}
			//forward topic creation requests to the worker pool
			distributor.send(data, 0);
		}
		logger.info("LB:{} exited listener loop.",lbId);
		clean();
	}
	
	public void clean(){
		//wait for worker threads to exit
		logger.debug("LB:{} will wait for worker threads to exit",lbId);
		try{
			for (Thread t : workers) {
				t.join();
			}
		}catch(InterruptedException e){}
		logger.debug("LB:{}  worker threads have exited",lbId);
	
		//set linger to 0. close all ZMQ sockets
		listener.setLinger(0);
		distributor.setLinger(0);
		controlSocket.setLinger(0);

		listener.close();
		distributor.close();
		controlSocket.close();
		
		//termiate context to release resources
		context.term();
		logger.debug("LB:{}  closed ZMQ sockets and context",lbId);

		//close CuratorClient connection to ZK
		CloseableUtils.closeQuietly(client);
		logger.debug("LB:{}  closed ZK connection",lbId);
		logger.info("LB:{} exited cleanly",lbId);
	}
	
	public static void main(String args[]){
		if(args.length<1){
			System.out.println("Usage: LoadBalancer zkConnector");
			return;
		}
		// Parse commandline arguments
		String zkConnector = args[0];

		// Instantiate LB
		LoadBalancer lb = new LoadBalancer(zkConnector);
		Thread lbThread= new Thread(lb);

		// Register callback to handle SIGINT and SIGTERM
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				// send SHUTDOWN_CONTROL_MSG interrupt LB
				ZMQ.Context context = ZMQ.context(1);
				ZMQ.Socket socket = context.socket(ZMQ.PUSH);
				socket.connect(String.format("tcp://127.0.0.1:%d", LoadBalancer.LISTENER_PORT));
				socket.send(String.format("%s", SHUTDOWN_CONTROL_MSG));
				try{
					lbThread.join();
				}catch(Exception e){}
				socket.setLinger(0);
				socket.close();
				context.term();
			}
		});
		// Start LB
		lbThread.start();
	}

}
