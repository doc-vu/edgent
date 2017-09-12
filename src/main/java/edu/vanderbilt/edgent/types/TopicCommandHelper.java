package edu.vanderbilt.edgent.types;

import com.google.flatbuffers.FlatBufferBuilder;

public class TopicCommandHelper {
	public static byte[] serialize(int type){
		FlatBufferBuilder builder = new FlatBufferBuilder(64);
		TopicCommand.startTopicCommand(builder);
		TopicCommand.addType(builder, type);
		int topicCommandOffset= TopicCommand.endTopicCommand(builder);
		builder.finish(topicCommandOffset);
		return builder.sizedByteArray();
	}

	public static byte[] serialize(int type, int containerCommand){
		FlatBufferBuilder builder = new FlatBufferBuilder(128);
		int containerCommandOffset= ContainerCommandHelper.offset(builder, containerCommand);
		int topicCommandOffset= TopicCommand.createTopicCommand(builder, type, containerCommandOffset);
		builder.finish(topicCommandOffset);
		return builder.sizedByteArray();
	}

	public static byte[] serialize(int type,int containerCommand,
			String containerId){
		FlatBufferBuilder builder = new FlatBufferBuilder(128);
		int containerCommandOffset= ContainerCommandHelper.offset(builder, 
				containerCommand, containerId);
		int topicCommandOffset= TopicCommand.createTopicCommand(builder, type, containerCommandOffset);
		builder.finish(topicCommandOffset);
		return builder.sizedByteArray();
	}
	
	public static byte[] serialize(int type, int containerCommand,
			String containerId,String ebId, String topicConnector){
		FlatBufferBuilder builder = new FlatBufferBuilder(128);
		int containerCommandOffset= ContainerCommandHelper.offset(builder, 
				containerCommand, containerId, ebId, topicConnector);
		int topicCommandOffset= TopicCommand.createTopicCommand(builder, type, containerCommandOffset);
		builder.finish(topicCommandOffset);
		return builder.sizedByteArray();
	}
	
	public static TopicCommand deserialize(byte[] data){
		java.nio.ByteBuffer buf= java.nio.ByteBuffer.wrap(data);
		return TopicCommand.getRootAsTopicCommand(buf);
	}
}
