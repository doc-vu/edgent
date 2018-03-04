package edu.vanderbilt.edgent.types;

import com.google.flatbuffers.FlatBufferBuilder;

public class DataSampleHelper {
	private FlatBufferBuilder builder;
	public DataSampleHelper(){
		builder= new FlatBufferBuilder(4096);
	}

	/**
	 * Serialize DataSample type using FlatBuffers
	 * @param sampleId DataSample's unique id 
	 * @param regionId DataSample's originating domain/region id
	 * @param runId  Experiment runId
	 * @param priority DataSample's priority 
	 * @param ts   
	 * @param payloadSize size of payload to create in addition to the header(24 Bytes)
	 * @return
	 */
	public byte[] serialize(int sampleId, int regionId, 
			int runId,int priority, long pubSendTs,long ebReceiveTs,
			String containerId, int payloadSize){
		builder.clear();
		int containerIdOffset= builder.createString(containerId);
		int payloadOffset= DataSample.createPayloadVector(builder, new byte[payloadSize]);
		int sample=DataSample.createDataSample(builder, sampleId, regionId, runId, priority,
				pubSendTs,ebReceiveTs,
				containerIdOffset, payloadOffset);
		builder.finish(sample);
		return builder.sizedByteArray();
	}
	
	/**
	 * De-serialize byte array into DataSample type using FlatBuffers 
	 * @param data  
	 * @return
	 */
	public static DataSample deserialize(byte[] data){
		java.nio.ByteBuffer buf= java.nio.ByteBuffer.wrap(data);
		return DataSample.getRootAsDataSample(buf);
	}

}
