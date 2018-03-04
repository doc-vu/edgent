package edu.vanderbilt.edgent.brokers;

import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import edu.vanderbilt.edgent.types.DataSample;
import edu.vanderbilt.edgent.types.DataSampleHelper;


public class TestReceiver {
	public static void main(String args[]){
		ZMQ.Context context= ZMQ.context(1);
		ZMQ.Socket receiver= context.socket(ZMQ.SUB);
		receiver.setHWM(0);
		receiver.connect("tcp://localhost:6666");
		receiver.subscribe("t1".getBytes());
		int count=0;
		System.out.println("Receiver will listen for data");
		while(count<1000){
			ZMsg msg= ZMsg.recvMsg(receiver);
			long receieveTs= System.currentTimeMillis();
			//De-serialize received data 
			DataSample sample = DataSampleHelper.deserialize(msg.getLast().getData());
			long latency=(receieveTs-sample.pubSendTs())/1000;
			count++;
			System.out.format("Received sample id:%d latency:%d(s)\n",sample.sampleId(),latency);
		}
		receiver.setLinger(0);
		receiver.close();
		context.close();
		context.term();
	}

}
