package edu.vanderbilt.edgent.types;

import com.google.flatbuffers.FlatBufferBuilder;

public class WorkerCommandHelper {

	public static byte[] serialize(int type){
		FlatBufferBuilder builder= new FlatBufferBuilder(64);
		WorkerCommand.startWorkerCommand(builder);
		WorkerCommand.addType(builder, type);
		int workerCommandOffset= WorkerCommand.endWorkerCommand(builder);
		builder.finish(workerCommandOffset);
		return builder.sizedByteArray();
	}

	public static byte[] serialize(int type,String ebId){
		FlatBufferBuilder builder= new FlatBufferBuilder(64);
		int ebIdOffset=builder.createString(ebId);
		int workerCommandOffset=WorkerCommand.createWorkerCommand(builder, type, ebIdOffset);
		builder.finish(workerCommandOffset);
		return builder.sizedByteArray();
	}

	public static WorkerCommand deserialize(byte[] data){
		java.nio.ByteBuffer buf= java.nio.ByteBuffer.wrap(data);
		return WorkerCommand.getRootAsWorkerCommand(buf);
	}
}
