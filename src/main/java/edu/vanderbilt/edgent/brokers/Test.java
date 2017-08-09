package edu.vanderbilt.edgent.brokers;

import org.zeromq.ZMQ;

public class Test {
   public static void main (String[] args) {
	   ZMQ.Context context= ZMQ.context(1);
	   ZMQ.Socket requester= context.socket(ZMQ.REQ);
	   requester.connect("tcp://localhost:6667");
	   for(int i=0;i<100;i++){
		   requester.send("hello",0);
		   String rep=requester.recvStr(0);
		   System.out.println("Received:"+rep);
	   }
	   requester.close();
	   context.close();
	   context.term();
   }
}
