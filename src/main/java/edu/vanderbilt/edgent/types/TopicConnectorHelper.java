package edu.vanderbilt.edgent.types;

import com.google.flatbuffers.FlatBufferBuilder;

public class TopicConnectorHelper {
	
	public static int offset(FlatBufferBuilder builder,String ebId, String topicConnector){
		/* Prase parts from topicConnector string: ebAddress, sendPort,
		receivePort and controlPort*/
		String[] connectorParts= topicConnector.split(",");
		String ebAddress= connectorParts[0];
		int receivePort= Integer.parseInt(connectorParts[1]);
		int sendPort= Integer.parseInt(connectorParts[2]);
		int controlPort= Integer.parseInt(connectorParts[3]);

		//get string offsets
		int ebIdOffset= builder.createString(ebId);
		int ebAddressOffset= builder.createString(ebAddress);
	
		//get topic connector's offset
		return TopicConnector.createTopicConnector(builder, ebIdOffset,
				ebAddressOffset, receivePort, sendPort, controlPort);
	}

	public static int offset(FlatBufferBuilder builder,TopicConnector connector){
		return offset(builder,connector.ebId(),TopicConnectorHelper.toString(connector));
	}

	public static String toString(TopicConnector connector){
		/* Returns ebAddress,receivePort,sendPort,controlPort as string. 
		 * Note: EbId is not included in this representation
		 */
		return String.format("%s,%d,%d,%d", 
				connector.ebAddress(),connector.receivePort(),connector.sendPort(),connector.controlPort());
	}
}
