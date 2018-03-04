package edu.vanderbilt.edgent.brokers;

import org.zeromq.ZMQ;
import edu.vanderbilt.edgent.types.DataSampleHelper;

public class TestSender {
	public static void main(String args[]) throws InterruptedException{
		ZMQ.Context context= ZMQ.context(1);
		ZMQ.Socket sender= context.socket(ZMQ.PUB);
		sender.setHWM(0);
		sender.connect("tcp://localhost:5555");
		DataSampleHelper dataSampleHelper= new DataSampleHelper();
		int count=0;
		Thread.sleep(10000);
		System.out.println("Sender will start sending data");
		while(count < 1000){
			count++;
			sender.sendMore("t1".getBytes());
			sender.send(dataSampleHelper.serialize(count, 0, 1, 1, System.currentTimeMillis(), -1, "sender", 4000));
			Thread.sleep(1000);
		    System.out.format("Sent %d samples\n",count);
		}
		sender.setLinger(0);
		sender.close();
		context.close();
		context.term();
	}

}
