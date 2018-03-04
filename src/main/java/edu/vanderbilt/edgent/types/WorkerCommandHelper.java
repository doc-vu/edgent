package edu.vanderbilt.edgent.types;

import com.google.flatbuffers.FlatBufferBuilder;

public class WorkerCommandHelper {
	private FlatBufferBuilder builder;

	public WorkerCommandHelper(){
		builder= new FlatBufferBuilder(128);
	}

	public byte[] serialize(int type){
		builder.clear();
		//start WorkerCommand builder
		WorkerCommand.startWorkerCommand(builder);
		//add Type
		WorkerCommand.addType(builder, type);
		//end WorkerCommand builder and return byte array
		int workerCommandOffset= WorkerCommand.endWorkerCommand(builder);
		builder.finish(workerCommandOffset);
		return builder.sizedByteArray();
	}

	public byte[] serialize(int type,TopicConnector connector){
		builder.clear();
		int topicConnectorOffset= TopicConnectorHelper.offset(builder, connector);
		int workerCommandOffset= WorkerCommand.createWorkerCommand(builder, type, topicConnectorOffset);
		builder.finish(workerCommandOffset);
		return builder.sizedByteArray();
	}

	public static WorkerCommand deserialize(byte[] data){
		java.nio.ByteBuffer buf= java.nio.ByteBuffer.wrap(data);
		return WorkerCommand.getRootAsWorkerCommand(buf);
	}
}
