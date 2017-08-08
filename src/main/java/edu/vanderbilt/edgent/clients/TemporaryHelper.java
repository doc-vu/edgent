package edu.vanderbilt.edgent.clients;

import java.util.HashMap;

public class TemporaryHelper {
	private static HashMap<String,Integer> topicReceivePortMap;
	private static HashMap<String,Integer> topicSendPortMap;
	static {
		topicSendPortMap= new HashMap<String,Integer>();
		topicSendPortMap.put("t1", 5000);
		topicSendPortMap.put("t2", 6000);

		topicReceivePortMap= new HashMap<String,Integer>();
		topicReceivePortMap.put("t1", 5001);
		topicReceivePortMap.put("t2", 6001);
	}
	
	public static int topicSendPort(String topic){
		return topicSendPortMap.get(topic);
	}
	
	public static int topicReceivePort(String topic) {
		return topicReceivePortMap.get(topic);
	}
}
