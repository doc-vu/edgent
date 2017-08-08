package edu.vanderbilt.edgent.brokers;

import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import edu.vanderbilt.edgent.util.PortList;
import edu.vanderbilt.edgent.util.UtilMethods;

/**
 * EdgeBroker managing local/intra-region data dissemination concerns.
 * @author kharesp
 */

public class EdgeBroker {
	private String ebId;
	//identifier of region to which this EB belongs
	private int regionId;
	private String ipAddress;

	private PortList ports;
	private ZMQ.Context context;
	private Map<String,Topic> hostedTopics;

	private Logger logger;
	
	public EdgeBroker(int regionId,int ioThreads){
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		ipAddress= UtilMethods.ipAddress();
		this.regionId=regionId;
		ebId=String.format("EB-%d-%s",regionId,ipAddress);

		
		context= ZMQ.context(ioThreads);
		hostedTopics= new HashMap<String,Topic>();
		ports= PortList.getInstance();
		
		logger.debug("Initialized EdgeBroker:{}",ebId);
	}
	
	/**
	 * Creates a topic which will be hosted on this EB. 
	 * @param topicName 
	 */
	public void createTopic(String topicName,int receivePort, int sendPort){
		logger.debug("EdgeBroker:{} creating topic:{}",ebId,topicName);
		if(!hostedTopics.containsKey(topicName)){
			//TODO acquire port numbers from portList
			Topic topic= new Topic(topicName,context,receivePort,sendPort);
			hostedTopics.put(topicName,topic);
			new Thread(topic).start();
			logger.info("EdgeBroker:{} created topic:{}",ebId,topicName);
		}else{
			logger.error("EdgeBroker:{} topic:{} already exists",ebId,topicName);
		}
	}

	/**
	 * Deletes a topic hosted on this EB.
	 * @param topicName
	 */
	public void deleteTopic(String topicName){
		logger.debug("EdgeBroker:{} deleting topic:{}",ebId,topicName);
		if(hostedTopics.containsKey(topicName))
		{
			Topic topic=hostedTopics.remove(topicName);
			topic.stop();
			//TODO release port numbers
			logger.info("EdgeBroker:{} deleted topic:{}", ebId,topicName);

		}else{
			logger.error("EdgeBroker:{} topic:{} does not exist",ebId,topicName);
		}
	}
	

	public static void main(String args[]){
		EdgeBroker eb= new EdgeBroker(1,3);
		eb.createTopic("t1",5000,5001);
		eb.createTopic("t2",6000,6001);
	}

}
