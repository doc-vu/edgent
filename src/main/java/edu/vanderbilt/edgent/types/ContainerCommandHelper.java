package edu.vanderbilt.edgent.types;

import com.google.flatbuffers.FlatBufferBuilder;

public class ContainerCommandHelper {

	public static byte[] serialize(int type){
		FlatBufferBuilder builder= new FlatBufferBuilder(64);
		int containerCommandOffset= offset(builder,type);
		builder.finish(containerCommandOffset);
		return builder.sizedByteArray();
	}

	public static int offset(FlatBufferBuilder builder, int type){
		//start ContainerCommand builder
		ContainerCommand.startContainerCommand(builder);
		//add type
		ContainerCommand.addType(builder, type);
		//return ContainerCommand offset
		return ContainerCommand.endContainerCommand(builder);
	}
	
	public static byte[] serialize(int type,String containerId){
		FlatBufferBuilder builder= new FlatBufferBuilder(128);
		int containerCommandOffset= offset(builder,type,containerId);
		builder.finish(containerCommandOffset);
		return builder.sizedByteArray();
	}
	
	public static int offset(FlatBufferBuilder builder,int type,String containerId){
		int containerIdOffset= builder.createString(containerId);
		//start ContainerCommand builder
		ContainerCommand.startContainerCommand(builder);
		//add Type
		ContainerCommand.addType(builder, type);
		//add containerId 
		ContainerCommand.addContainerId(builder, containerIdOffset);
		//close ContainerCommand builder and return offset
		return ContainerCommand.endContainerCommand(builder);
	}

	public static byte[] serialize(int type,String containerId,String ebId,String topicConnector){
		FlatBufferBuilder builder= new FlatBufferBuilder(128);
		int containerCommandOffset= offset(builder,type,containerId,ebId,topicConnector);
		builder.finish(containerCommandOffset);
		return builder.sizedByteArray();
	}
	
	public static int offset(FlatBufferBuilder builder,int type,
			String containerId,String ebId, String topicConnector){
		
		int containerIdOffset= builder.createString(containerId);
		int topicConnectorOffset= TopicConnectorHelper.offset(builder, ebId, topicConnector);
		//start ContainerCommand builder
		ContainerCommand.startContainerCommand(builder);
		//add type
		ContainerCommand.addType(builder, type);
		//add containerId
		ContainerCommand.addContainerId(builder, containerIdOffset);
		//add topicConnector
		ContainerCommand.addTopicConnector(builder, topicConnectorOffset);
		//close ContainerCommand builder and return offset
		return ContainerCommand.endContainerCommand(builder);
	}
	
	public static byte[] serialize(ContainerCommand command){
		int type=command.type();
		String containerId=command.containerId();
		TopicConnector topicConnector= command.topicConnector();
		if(containerId==null && topicConnector==null){
			return serialize(type);
		}
		else if(topicConnector==null){
			return serialize(type,containerId);
		}else{
			String strTopicConnector=String.format("%s,%d,%d,%d", topicConnector.ebAddress(),
					topicConnector.receivePort(),topicConnector.sendPort(),topicConnector.controlPort());
			return serialize(type,containerId,topicConnector.ebId(),strTopicConnector);
		}
	}
	
	public static ContainerCommand deserialize(byte[] data){
		java.nio.ByteBuffer buf= java.nio.ByteBuffer.wrap(data);
		return ContainerCommand.getRootAsContainerCommand(buf);
	}
}
