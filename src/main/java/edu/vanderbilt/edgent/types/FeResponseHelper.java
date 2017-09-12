package edu.vanderbilt.edgent.types;

import java.util.HashMap;
import java.util.Map.Entry;
import com.google.flatbuffers.FlatBufferBuilder;

public class FeResponseHelper {
	
	public static byte[] serialize(int code,String msg){
		FlatBufferBuilder builder= new FlatBufferBuilder(64);
		int msgOffset= builder.createString(msg);

		//start FeResponse builder
		FeResponse.startFeResponse(builder);
		//add code
		FeResponse.addCode(builder, code);
		//add message
		FeResponse.addMsg(builder, msgOffset);
		//close FeResponse builder and return byte array
		int feResponseOffset= FeResponse.endFeResponse(builder);
		builder.finish(feResponseOffset);
		return builder.sizedByteArray();
	}
	
	public static byte[] serialize(int code){
		FlatBufferBuilder builder= new FlatBufferBuilder(64);
		//start FeResponse builder
		FeResponse.startFeResponse(builder);
		//add code
		FeResponse.addCode(builder, code);
		//close FeResponse builder and return byte array
		int feResponseOffset= FeResponse.endFeResponse(builder);
		builder.finish(feResponseOffset);
		return builder.sizedByteArray();
	}

	public static byte[] serialize(int code,HashMap<String,String> ebConnectors){
		FlatBufferBuilder builder= new FlatBufferBuilder(1024);
	
		//create TopicConnectors
		int i=0;
		int[] topicConnectorOffsets= new int[ebConnectors.size()];
		for(Entry<String, String> topicConnector: ebConnectors.entrySet()){
			String ebId=topicConnector.getKey();
			String[] parts= topicConnector.getValue().split(",");
			String ebAddress= parts[0];
			int receivePort=Integer.parseInt(parts[1]);
			int sendPort= Integer.parseInt(parts[2]);
			int controlPort= Integer.parseInt(parts[3]);

			int ebIdOffset= builder.createString(ebId);
			int ebAddressOffset= builder.createString(ebAddress);
			int topicConnectorOffset= TopicConnector.createTopicConnector(builder, ebIdOffset, 
					ebAddressOffset, receivePort, sendPort, controlPort);
			topicConnectorOffsets[i++]=topicConnectorOffset;
		}
		int connectorsOffset= FeResponse.createConnectorsVector(builder, topicConnectorOffsets);

		//start FeResponse builder
		FeResponse.startFeResponse(builder);
		//add code
		FeResponse.addCode(builder, code);
		//add connectors
		FeResponse.addConnectors(builder, connectorsOffset);
		//close FeResponse builder and return byte array
		int feResponseOffset= FeResponse.endFeResponse(builder);
		builder.finish(feResponseOffset);
		return builder.sizedByteArray();
	}

	public static FeResponse deserialize(byte[] data){
		java.nio.ByteBuffer buf= java.nio.ByteBuffer.wrap(data);
		return FeResponse.getRootAsFeResponse(buf);
	}

}
