package edu.vanderbilt.edgent.brokers;


import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import edu.vanderbilt.edgent.types.DataSample;
import edu.vanderbilt.edgent.types.DataSampleHelper;


public class TestTopic {
	
	public static void main(String args[]){
		ZMQ.Context context= ZMQ.context(1);
		ZMQ.Socket receiver= context.socket(ZMQ.SUB);
		receiver.setHWM(0);
		ZMQ.Socket sender= context.socket(ZMQ.PUB);
		sender.setHWM(0);
		receiver.bind(String.format("tcp://localhost:%d",5555));
		receiver.subscribe("t1".getBytes());
		sender.bind(String.format("tcp://localhost:%d", 6666));
		DataSampleHelper dataSampleHelper= new DataSampleHelper();
		int count=0;
		System.out.println("Topic will listen for data");
		try{
			while (count < 1000) {
				ZMsg receivedMsg = ZMsg.recvMsg(receiver);
				String msgTopic = new String(receivedMsg.getFirst().getData());
				byte[] msgContent = receivedMsg.getLast().getData();

				long ebReceiveTs = System.currentTimeMillis();
				DataSample sample = DataSampleHelper.deserialize(msgContent);
				System.out.format("Topic received sample:%d\n", sample.sampleId());

				// bogus processing operation
				//bogus("stress-ng --cpu 1 --cpu-method matrixprod --timeout 3s");

				sender.sendMore(msgTopic);
				sender.send(msgContent);
				sender.send(dataSampleHelper.serialize(sample.sampleId(), sample.regionId(), sample.runId(),
						sample.priority(), sample.pubSendTs(), ebReceiveTs, sample.containerId(),
						sample.payloadLength()));

				count++;
				
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		
		receiver.setLinger(0);
		sender.setLinger(0);
		receiver.close();
		sender.close();
		context.close();
		context.term();
	}
	
	public static void bogus(String command) throws Exception{
			Process p = Runtime.getRuntime().exec(command);
			p.waitFor();
	}

}
