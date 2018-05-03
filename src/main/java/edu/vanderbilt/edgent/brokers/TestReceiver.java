package edu.vanderbilt.edgent.brokers;

import java.io.IOException;

import org.zeromq.ZCert;
import org.zeromq.ZConfig;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import edu.vanderbilt.edgent.types.DataSample;
import edu.vanderbilt.edgent.types.DataSampleHelper;


public class TestReceiver {
	public static void main(String args[]) throws IOException{
		ZMQ.Context context= ZMQ.context(1);
		ZMQ.Socket receiver= context.socket(ZMQ.SUB);
		receiver.setHWM(0);

		ZCert clientCert = new ZCert();
		clientCert.apply(receiver);
		ZConfig config=ZConfig.load("/home/kharesp/testCert.txt");
		receiver.setCurveServerKey(config.getValue("curve/public-key").getBytes());

		receiver.connect("tcp://localhost:6666");
		receiver.subscribe("t1".getBytes());
		int count=0;
		System.out.println("Receiver will listen for data");
		while(count<1000){
			ZMsg msg= ZMsg.recvMsg(receiver);
			long receieveTs= System.currentTimeMillis();
			//De-serialize received data 
			DataSample sample = DataSampleHelper.deserialize(msg.getLast().getData());
			long latency=(receieveTs-sample.pubSendTs());
			count++;
			System.out.format("Received sample id:%d latency:%d(ms)\n",sample.sampleId(),latency);
		}
		receiver.setLinger(0);
		receiver.close();
		context.close();
		context.term();
	}

}
