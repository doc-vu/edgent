package edu.vanderbilt.edgent.endpoints.subscriber;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Deduplication {
	//publisher is assumed to have left if no data is received for EXPIRY_PERIOD_SECONDS
	private static final int PUBLISHER_EXPIRY_PERIOD_SECONDS=300;

	//Publisher for which this deduplication logic is being performed
	private String pubId;
	//uuid of most recently seen message Id for this publisher 
	private int uuid;
	//Timestamp at which most recently received message was logged 
	private long mostRecentMsgTs;

	//current number of messages received for this publisher 
	private int receiveCount;
	//number of lost messages
	private int lostSampleCount;
	//Specifies lower bound of the missing sample range 
	private int missingRangeLowerBound;
	//Sepcifies upper bound of the missing sample range
	private int missingRangeUpperBound;
	//flag that shows whether duplicate messages are being received for this publisher
	private boolean receivingDuplicates;
	private Logger logger;

	public Deduplication(String pubId, int uuid){
		logger= LogManager.getLogger(this.getClass().getName());
		this.pubId=pubId;
		this.uuid=uuid;
		mostRecentMsgTs=-1;
		receiveCount=1;
		lostSampleCount=0;
		missingRangeLowerBound=-1;
		missingRangeUpperBound=-1;
		receivingDuplicates=false;
		logger.debug("Deduplication:{} initialized",pubId);
	}

	/* Returns true if a message is seen for the first time. 
	 * False is returned when a duplicate message is seen.
	 */
	public boolean update(int update){
		//log the timestamp at which this message was seen
		mostRecentMsgTs=System.currentTimeMillis();
		
		/*if new message's sampleId lies within the missing range of missed samples, 
		return true to mark this sample as seen for the first time*/
		if(update>=missingRangeLowerBound && update<=missingRangeUpperBound){
		    receiveCount++;
			return true;
		}
		/* If new message's sampleId is less than current uuid and it does not lie within 
		 * the missing range of samples, then we have seen this message before. 
		 * We can mark this as duplicate. 
		 */
		else if(update <= uuid){
			//set receivingDuplicates flag to true
			receivingDuplicates=true;
			return false;
		}else{
			//check if messages were lost
			int missingRange = update - (uuid + 1);
			if (missingRange > 0) {
				lostSampleCount+=missingRange;
				logger.error("Deduplication:{} lost {} messages.Total number of lost messages:{}",
						pubId, missingRange,lostSampleCount);
				missingRangeLowerBound = uuid + 1;
				missingRangeUpperBound = update - 1;
			}
			//update the current uuid with this message's sampleId
			uuid=update;
			//update total number of samples received for this publisher 
			receiveCount++;
			return true;
		}
	}

	/*
	 * If no update is received from this publisher for 
	 * PUBLISHER_EXPIRY_PERIOD then this publisher is marked as expried.
	 */
	public boolean expired(){
		long delay=System.currentTimeMillis()-mostRecentMsgTs;
		if(delay>(PUBLISHER_EXPIRY_PERIOD_SECONDS*1000)){
			return true;
		}else{
			return false;
		}
	}
	
	public boolean receivingDuplicates(){
		return receivingDuplicates;
	}

	public void resetReceivingDuplicates(){
		receivingDuplicates=false;
	}
	
	public int receiveCount(){
		return receiveCount;
	}
	
	public int lostSampleCount(){
		return lostSampleCount;
	}
}
