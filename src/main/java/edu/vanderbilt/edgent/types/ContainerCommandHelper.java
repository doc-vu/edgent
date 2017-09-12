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
		ContainerCommand.startContainerCommand(builder);
		ContainerCommand.addType(builder, type);
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
		ContainerCommand.startContainerCommand(builder);
		ContainerCommand.addType(builder, type);
		ContainerCommand.addContainerId(builder, containerIdOffset);
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
		int ebIdOffset= builder.createString(ebId);

		String[] connectorParts= topicConnector.split(",");
		String ebAddress= connectorParts[0];
		int receivePort= Integer.parseInt(connectorParts[1]);
		int sendPort= Integer.parseInt(connectorParts[2]);
		int controlPort= Integer.parseInt(connectorParts[3]);
		int ebAddressOffset= builder.createString(ebAddress);
		
		int topicConnectorOffset= TopicConnector.createTopicConnector(builder, ebIdOffset,
				ebAddressOffset, receivePort, sendPort, controlPort);
		ContainerCommand.startContainerCommand(builder);
		ContainerCommand.addType(builder, type);
		ContainerCommand.addContainerId(builder, containerIdOffset);
		ContainerCommand.addTopicConnector(builder, topicConnectorOffset);
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
