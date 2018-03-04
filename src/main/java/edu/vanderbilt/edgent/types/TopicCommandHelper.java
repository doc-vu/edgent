package edu.vanderbilt.edgent.types;

import com.google.flatbuffers.FlatBufferBuilder;

public class TopicCommandHelper {
	private FlatBufferBuilder builder;

	public TopicCommandHelper(){
		builder= new FlatBufferBuilder(64);
	}

	public byte[] serialize(int type){
		builder.clear();
		//start TopicCommand builder
		TopicCommand.startTopicCommand(builder);
		//add type
		TopicCommand.addType(builder, type);
		//end TopicCommand builder and return byte array
		int topicCommandOffset= TopicCommand.endTopicCommand(builder);
		builder.finish(topicCommandOffset);
		return builder.sizedByteArray();
	}

	public byte[] serialize(int type, int containerCommand){
		builder.clear();
		//get ContainerCommand offset
		int containerCommandOffset= ContainerCommandHelper.offset(builder, containerCommand);
		//build TopicCommand and return byte array
		int topicCommandOffset= TopicCommand.createTopicCommand(builder, type, containerCommandOffset);
		builder.finish(topicCommandOffset);
		return builder.sizedByteArray();
	}

	public byte[] serialize(int type,int containerCommand,
			String containerId){
		builder.clear();
		//get ContainerCommand offset
		int containerCommandOffset= ContainerCommandHelper.offset(builder, 
				containerCommand, containerId);
		//build TopicCommand and return byte array
		int topicCommandOffset= TopicCommand.createTopicCommand(builder, type, containerCommandOffset);
		builder.finish(topicCommandOffset);
		return builder.sizedByteArray();
	}
	
	public byte[] serialize(int type, int containerCommand,
			String containerId,String ebId, String topicConnector){
		builder.clear();
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
