package edu.vanderbilt.edgent.fe;

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import edu.vanderbilt.edgent.util.UtilMethods;

public class Frontend {
	//ZMQ Context
	private ZContext context;
	//Router and Dealer sockets for supporting multi-threaded operation
	private ZMQ.Socket listener;
	private ZMQ.Socket distributor;
	//Front facing Router and backend Dealer socket endpoints
	public static final int LISTENER_PORT=5555;
	public static final String INPROC_CONNECTOR="inproc://workers";
	
	//FE Request Commands
	public static final String CONNECTION_REQUEST="connect";
	public static final String DISCONNECTION_REQUEST="disconnect";
	
	//Number of concurrent threads servicing incoming requests 
	public static final int WORKER_POOL_SIZE=5;
	private List<Thread> workers;

	private String feId;
	private Logger logger;

	public Frontend(int regionId, ZContext context,String zkConnector){
		logger=LogManager.getLogger(this.getClass().getSimpleName());
		feId=String.format("FE-%d-%s",regionId,UtilMethods.ipAddress());

		//initialize front facing Router socket
		this.context= context;
		listener=context.createSocket(ZMQ.ROUTER);
		listener.bind(String.format("tcp://*:%d",LISTENER_PORT));

		//initialize backend distributer socket
		distributor=context.createSocket(ZMQ.DEALER);
		distributor.bind(INPROC_CONNECTOR);
		
		//create  worker threads to process incoming requests concurrently
		workers= new ArrayList<Thread>();
		for(int i=0;i<WORKER_POOL_SIZE;i++){
			workers.add(new Thread(new WorkerThread(ZContext.shadow(context),zkConnector)));
		}

		logger.debug("Initalized FE:{}",feId);
	}
	
	public void start(){
		//start worker threads
		logger.debug("FE:{} starting worker threads",feId);
		for(Thread t:workers){
			t.start();
		}
		
		//start ZMQ proxy to listen for incoming requests and forward them to worker pool
		logger.debug("FE:{} will start listening for incoming requests at port:{}",
				feId,LISTENER_PORT);
		ZMQ.proxy(listener,distributor, null);
		
		//FE was terminated, perform clean-up
		logger.debug("FE:{} proxy was terminated",feId);
		context.destroy();
		
		logger.debug("FE:{} will wait for worker threads to exit",feId);
		try{
			for (Thread t : workers) {
				t.interrupt();
				t.join();
			}
		}catch(InterruptedException e){}

		logger.debug("FE:{} exited",feId);
	}
	
	public static void main(String args[]){
		if(args.length<2)
		{
			System.out.println("Usage: FrontEnd regionId zkConnector");
			return;
		}
		try{
			int regionId= Integer.parseInt(args[0]);
			String zkConnector=args[1];
		
			//ZMQ Context for FE
			ZContext context= new ZContext();

			//register callback to safely handle SIGINT SIGTERM
			Runtime.getRuntime().addShutdownHook(new Thread() {
		         @Override
		         public void run() {
		            context.destroy();
		         }
		      });	
			
			//start FE
			Frontend fe= new Frontend(regionId,context,zkConnector);
			fe.start();
			
		}catch(NumberFormatException e){
			e.printStackTrace();
		}
	}
}
