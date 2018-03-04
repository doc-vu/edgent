package edu.vanderbilt.edgent.types;

import com.google.flatbuffers.FlatBufferBuilder;

public class FeRequestHelper {

	private FlatBufferBuilder builder;
	public FeRequestHelper(){
		builder=new FlatBufferBuilder(128);
	}
	
	public byte[] serialize(int type,String topicName,String endpointType,
			String containerId,String ebId,String experimentType){
		builder.clear();
		//get builder string offsets
		int topicNameOffset= builder.createString(topicName);
		int endpointTypeOffset= builder.createString(endpointType);
		int containerIdOffset= builder.createString(containerId);
		int ebIdOffset= builder.createString(ebId);
		int experimentTypeOffset=builder.createString(experimentType);
		//get FeRequest offset
		int feRequestOffset= FeRequest.createFeRequest(builder, type, topicNameOffset,
				endpointTypeOffset, containerIdOffset, ebIdOffset,experimentTypeOffset);
		builder.finish(feRequestOffset);
		return builder.sizedByteArray();
	}
	
	public byte[] serialize(int type, String topicName, String endpointType,
			String containerId){
		builder.clear();
		//get builder string offsets
		int topicNameOffset= builder.createString(topicName);
		int endpointTypeOffset= builder.createString(endpointType);
		int containerIdOffset= builder.createString(containerId);
		//start FeRequest builder
		FeRequest.startFeRequest(builder);
		//add type
		FeRequest.addType(builder, type);
		//add topicName
		FeRequest.addTopicName(builder, topicNameOffset);
		//add endpointType
		FeRequest.addEndpointType(builder, endpointTypeOffset);
		//add containerId
		FeRequest.addContainerId(builder, containerIdOffset);
		//close FeRequest builder and return byte array
		int feRequestOffset= FeRequest.endFeRequest(builder);
		builder.finish(feRequestOffset);
		return builder.sizedByteArray();
	}

	public static FeRequest deserailize(byte[] data){
		java.nio.ByteBuffer buf= java.nio.ByteBuffer.wrap(data);
		return FeRequest.getRootAsFeRequest(buf);
	}
	
}
