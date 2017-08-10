package edu.vanderbilt.edgent.brokers;

import org.zeromq.ZMQ;

public class Test {
   public static void main (String[] args) throws InterruptedException {
	   ZMQ.Context context= ZMQ.context(1);
	   ZMQ.Socket requester= context.socket(ZMQ.REQ);
	   requester.connect("tcp://localhost:5552");
	   for(int i=0;i<5;i++){
		   requester.send("connect,t1,pub,ip",0);
		   String rep=requester.recvStr(0);
		   System.out.println("Received:"+rep);
		   Thread.sleep(1000);
	   }
	   requester.close();
	   context.close();
	   context.term();
   }
}
