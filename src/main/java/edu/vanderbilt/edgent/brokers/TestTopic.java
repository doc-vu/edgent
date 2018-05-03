package edu.vanderbilt.edgent.brokers;


import java.io.IOException;

import org.zeromq.ZAuth;
import org.zeromq.ZCertStore;
import org.zeromq.ZConfig;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import edu.vanderbilt.edgent.types.DataSample;
import edu.vanderbilt.edgent.types.DataSampleHelper;


public class TestTopic {
	
	public static void main(String args[]) throws IOException{
		//ZMQ.Context context= ZMQ.context(1);
		ZContext context= new ZContext(1);
		//ZAuth auth= new ZAuth(context);
		//auth.setVerbose(true);
		//auth.replies(true);
		
		//auth.configureCurve(ZAuth.CURVE_ALLOW_ANY);

	
		
		ZConfig config=ZConfig.load("/home/kharesp/testCert.txt");

		ZMQ.Socket receiver= context.createSocket(ZMQ.SUB);
		receiver.setHWM(0);
		//receiver.setZapDomain("global".getBytes());
		receiver.setCurveServer(true);
		receiver.setCurvePublicKey(config.getValue("curve/public-key").getBytes());
		receiver.setCurveSecretKey(config.getValue("curve/secret-key").getBytes());
		
		
		ZMQ.Socket sender= context.createSocket(ZMQ.PUB);
		sender.setHWM(0);
		sender.setCurveServer(true);
		sender.setCurvePublicKey(config.getValue("curve/public-key").getBytes());
		sender.setCurveSecretKey(config.getValue("curve/secret-key").getBytes());

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
				//sender.send(msgContent);
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
		//auth.close();
		//auth.destroy();
		context.close();
	}
	
	public static void bogus(String command) throws Exception{
			Process p = Runtime.getRuntime().exec(command);
			p.waitFor();
	}

}
