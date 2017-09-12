package edu.vanderbilt.edgent.types;

import com.google.flatbuffers.FlatBufferBuilder;

public class TopicCommandHelper {

	public static byte[] serialize(int type){
		FlatBufferBuilder builder = new FlatBufferBuilder(64);
		//start TopicCommand builder
		TopicCommand.startTopicCommand(builder);
		//add type
		TopicCommand.addType(builder, type);
		//end TopicCommand builder and return byte array
		int topicCommandOffset= TopicCommand.endTopicCommand(builder);
		builder.finish(topicCommandOffset);
		return builder.sizedByteArray();
	}

	public static byte[] serialize(int type, int containerCommand){
		FlatBufferBuilder builder = new FlatBufferBuilder(128);
		//get ContainerCommand offset
		int containerCommandOffset= ContainerCommandHelper.offset(builder, containerCommand);
		//build TopicCommand and return byte array
		int topicCommandOffset= TopicCommand.createTopicCommand(builder, type, containerCommandOffset);
		builder.finish(topicCommandOffset);
		return builder.sizedByteArray();
	}

	public static byte[] serialize(int type,int containerCommand,
			String containerId){
		FlatBufferBuilder builder = new FlatBufferBuilder(128);
		//get ContainerCommand offset
		int containerCommandOffset= ContainerCommandHelper.offset(builder, 
				containerCommand, containerId);
		//build TopicCommand and return byte array
		int topicCommandOffset= TopicCommand.createTopicCommand(builder, type, containerCommandOffset);
		builder.finish(topicCommandOffset);
		return builder.sizedByteArray();
	}
	
	public static byte[] serialize(int type, int containerCommand,
			String containerId,String ebId, String topicConnector){
		FlatBufferBuilder builder = new FlatBufferBuilder(128);
		//get ContainerCommand offset
		int containerCommandOffset= ContainerCommandHelper.offset(builder, 
				containerCommand, containerId, ebId, topicConnector);
		//build TopicCommand and return byte array
		int topicCommandOffset= TopicCommand.createTopicCommand(builder, type, containerCommandOffset);
		builder.finish(topicCommandOffset);
		return builder.sizedByteArray();
	}
	
	public static TopicCommand deserialize(byte[] data){
		java.nio.ByteBuffer buf= java.nio.ByteBuffer.wrap(data);
		return TopicCommand.getRootAsTopicCommand(buf);
	}
}
