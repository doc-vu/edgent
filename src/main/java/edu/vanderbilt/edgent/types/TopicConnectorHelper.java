package edu.vanderbilt.edgent.types;

public class TopicConnectorHelper {

	public static String toString(TopicConnector connector){
		return String.format("%s,%d,%d,%d", 
				connector.ebAddress(),connector.receivePort(),connector.sendPort(),connector.controlPort());
	}

}
