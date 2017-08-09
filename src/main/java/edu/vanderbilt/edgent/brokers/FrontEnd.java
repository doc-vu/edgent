package edu.vanderbilt.edgent.brokers;

import java.util.ArrayList;
import java.util.List;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

public class FrontEnd {
	private static final int REQ_PORT_NUM=6667;
	private static final String INPROC_CONNECTOR="inproc://workers";
	private static final int POOL_SIZE=5;
	private ZMQ.Context context;
	private ZMQ.Socket receiver;
	private ZMQ.Socket distributer;
	private List<Thread> workers;

	
	public FrontEnd(){
		context=ZMQ.context(1);
		receiver=context.socket(ZMQ.ROUTER);
		distributer=context.socket(ZMQ.DEALER);
		receiver.bind(String.format("tcp://*:%d",REQ_PORT_NUM));
		distributer.bind(INPROC_CONNECTOR);
		
		workers= new ArrayList<Thread>();
		for(int i=0;i<POOL_SIZE;i++){
			Thread worker= new Thread(new Worker(context));
			workers.add(worker);
			worker.start();
		}
	}
	
	public void start(){
		ZMQ.proxy (receiver, distributer, null);
	}

	public void shutdown(){
		context.close();
		context.term();
		try{
			for(Thread t:workers){
				t.interrupt();
				t.join();
			}
		}catch(InterruptedException e){
			
		}
	}

	public static class Worker implements Runnable{
		private ZContext context;
		public Worker(ZMQ.Context context){
			this.context=context;
		}
		@Override
		public void run() {
			ZMQ.Socket socket=context.socket(ZMQ.REP);
			socket.connect(INPROC_CONNECTOR);
			while(!Thread.currentThread().isInterrupted()){
				try{
					byte[] req=socket.recv(0);
					System.out.println(Thread.currentThread().getName()+":received request:"+ new String(req));
					socket.send("World",0);
				}catch(ZMQException e){
					if (e.getErrorCode () == ZMQ.Error.ETERM.getCode ()) {
                    	System.out.println(e.getMessage());
                        break;
                    }
				}
			}
			System.out.println(Thread.currentThread().getName()+": closing socket");
			socket.close();
		}
	}

	public static void main(String args[]){
		FrontEnd fe= new FrontEnd();
		Runtime.getRuntime().addShutdownHook(new Thread() {
	         @Override
	         public void run() {
	            System.out.println("W: interrupt received, killing server…");
	            fe.shutdown();
	         }
	      });
		fe.start();
	}
}
