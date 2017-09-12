package edu.vanderbilt.edgent.endpoints;

import java.util.HashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;
import edu.vanderbilt.edgent.fe.Frontend;
import edu.vanderbilt.edgent.types.ContainerCommand;
import edu.vanderbilt.edgent.types.ContainerCommandHelper;
import edu.vanderbilt.edgent.types.FeRequestHelper;
import edu.vanderbilt.edgent.types.FeResponse;
import edu.vanderbilt.edgent.types.FeResponseHelper;
import edu.vanderbilt.edgent.types.TopicConnector;
import edu.vanderbilt.edgent.types.WorkerCommandHelper;
import edu.vanderbilt.edgent.util.Commands;
import edu.vanderbilt.edgent.util.PortList;
import edu.vanderbilt.edgent.util.UtilMethods;

public abstract class Container implements Runnable{
	

	//Base command and queue port numbers for Subscriber container 
	public static final int SUBSCRIBER_COMMAND_BASE_PORT_NUM=5000;
	public static final int SUBSCRIBER_QUEUE_BASE_PORT_NUM=6000;
	//Base command and queue port numbers for Publisher container 
	public static final int PUBLISHER_COMMAND_BASE_PORT_NUM=8000;
	public static final int PUBLISHER_QUEUE_BASE_PORT_NUM=9000;
	//Endpoint types
  	public static final String ENDPOINT_TYPE_SUB="sub";
  	public static final String ENDPOINT_TYPE_PUB="pub";
  	
  	public static final String FOR_ALL_CONTAINERS="all";
  	
	//ZMQ context
	protected ZMQ.Context context;
	//ZMQ.REQ socket to query FE
	private ZMQ.Socket feSocket;
	//ZMQ.PUB socket to send control messages
	private ZMQ.Socket commandSocket;
	//ZMQ.PULL socket to receive commands for the container 
	private ZMQ.Socket queueSocket;

	//Connector for container's commandSocket
	protected String commandConnector;
	//Connector for container's queue
	protected String queueConnector;
	
	private boolean onConnectedCalled;

	private int workerUuid;
	//Map to hold references to Workers 
	protected HashMap<String,Worker> workers;
	//Map to hold references to Worker threads
	protected HashMap<String,Thread> workerThreads;

	protected String endpointType;
	protected String topicName;
	protected String containerId;
	protected Logger logger;

	public Container( String topicName,String endpointType,int id){
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		this.topicName=topicName;
		this.endpointType=endpointType;

		containerId=String.format("%s-%s-%s-%d",endpointType,topicName,
				UtilMethods.hostName(),id);

		workerUuid=0;
		workers=new HashMap<String,Worker>();
		workerThreads=new HashMap<String,Thread>();
		
		onConnectedCalled=false;
		
		this.context=ZMQ.context(1);

		if(endpointType.equals(ENDPOINT_TYPE_SUB)){
			commandConnector = String.format("tcp://*:%d", (SUBSCRIBER_COMMAND_BASE_PORT_NUM + id));
			queueConnector = String.format("tcp://*:%d", (SUBSCRIBER_QUEUE_BASE_PORT_NUM + id));
		}
		if(endpointType.equals(ENDPOINT_TYPE_PUB)){
			commandConnector = String.format("tcp://*:%d", (PUBLISHER_COMMAND_BASE_PORT_NUM + id));
			queueConnector = String.format("tcp://*:%d", (PUBLISHER_QUEUE_BASE_PORT_NUM + id));
		}
		logger.debug("Container:{} initialized",containerId);
	}

	@Override
	public void run() {
		logger.info("Container:{} started", containerId);
		String feAddress= Frontend.FE_LOCATIONS.get(UtilMethods.regionId());
		System.out.println(feAddress);
		if(feAddress==null){
			logger.error("Container:{} could not locate it's region's FE(Region Id:{}). Exiting.",
					containerId,UtilMethods.regionId());
			context.term();
			return;
		}
		feSocket=context.socket(ZMQ.REQ);
		feSocket.connect(String.format("tcp://%s:%d",feAddress,PortList.FE_LISTENER_PORT));

		//create and bind container's queue socket 
		queueSocket= context.socket(ZMQ.PULL);
		queueSocket.bind(queueConnector);

		//create and bind container's command socket
		commandSocket= context.socket(ZMQ.PUB);
		commandSocket.bind(commandConnector);

		FeResponse resp= queryFe();

		if(resp.code()==Frontend.RESPONSE_ERROR){
			cleanupZMQ();
			return;
		}
		//container implementation specific intialization
		initialize();
		
		
		//start workers
		for(int i=0;i<resp.connectorsLength();i++){
			TopicConnector connector= resp.connectors(i);
			String ebId= connector.ebId();
			String topicConnector=String.format("%s,%d,%d,%d",connector.ebAddress(),
					connector.receivePort(),connector.sendPort(),connector.controlPort());
			workerUuid++;
			Worker worker=createWorker(workerUuid,ebId,topicConnector);
			workers.put(ebId, worker);
			workerThreads.put(ebId, new Thread(worker));
			workerThreads.get(ebId).start();
		}

		logger.info("Container:{} will start processing incoming commands",containerId );
		while (!Thread.currentThread().isInterrupted()) {
			try{
				System.out.println("Conatiner will listen");
				ContainerCommand command= ContainerCommandHelper.deserialize(queueSocket.recv());
				int commandType=command.type();

				//exit command
				if(commandType==Commands.CONTAINER_EXIT_COMMAND){
					System.out.println("CONTAINER IS EXITING");
					logger.info("Container:{} received CONTAINER_EXIT_COMMAND:{}. Will exit listener loop.", containerId,
							Commands.CONTAINER_EXIT_COMMAND);
					break;
				}
				//create worker command
				else if(commandType==Commands.CONTAINER_CREATE_WORKER_COMMAND){
					logger.info("Container:{} received CONTAINER_CREATE_WORKER_COMMAND:{}", containerId,
							Commands.CONTAINER_CREATE_WORKER_COMMAND);
					String targetContainerId= command.containerId();
					if(targetContainerId.equals(FOR_ALL_CONTAINERS) || targetContainerId.equals(containerId)){
						TopicConnector topicConnector= command.topicConnector();
						String strTopicConnector=String.format("%s,%d,%d,%d", topicConnector.ebAddress(),
								topicConnector.receivePort(),topicConnector.sendPort(),topicConnector.controlPort());
						connect(topicConnector.ebId(),strTopicConnector);
					}
				}
				//delete worker command
				else if(commandType==Commands.CONTAINER_DELETE_WORKER_COMMAND){
					logger.info("Container:{} received CONTAINER_DELETE_WORKER_COMMAND:{}" , containerId,
							Commands.CONTAINER_DELETE_WORKER_COMMAND);
					String targetContainerId= command.containerId();
					if(targetContainerId.equals(FOR_ALL_CONTAINERS) || targetContainerId.equals(containerId)){
						TopicConnector topicConnector= command.topicConnector();
						disconnect(topicConnector.ebId());
					}
				}
				//worker connected command
				else if (commandType==Commands.CONTAINER_WORKER_CONNECTED_COMMAND) {
					logger.info("Container:{} received CONTAINER_WORKER_CONNECTED_COMMAND:{}", containerId,
							Commands.CONTAINER_WORKER_CONNECTED_COMMAND);
					if(!onConnectedCalled){
						// check if all workers are in connected state
						if (workers.values().stream().allMatch(w -> w.connected() == Worker.STATE_CONNECTED)) {
							onConnected();
							onConnectedCalled=true;
						}
					}
				}
				//worker disconnected command
				else if (commandType==Commands.CONTAINER_WORKER_DISCONNECTED_COMMAND){
					logger.info("Container:{} received CONTAINER_WORKER_DISCONNECTED_COMMAND:{}", containerId,
							Commands.CONTAINER_WORKER_DISCONNECTED_COMMAND);
					TopicConnector topicConnector=command.topicConnector(); 
					disconnection(topicConnector.ebId());
				}
				//worker exited command
				else if (commandType==Commands.CONTAINER_WORKER_EXITED_COMMAND) {
					logger.info("Container:{} received CONTAINER_WORKER_EXITED_COMMAND:{}", containerId,
							Commands.CONTAINER_WORKER_EXITED_COMMAND);
					TopicConnector topicConnector=command.topicConnector(); 
					exited(topicConnector.ebId());
				}else{
					logger.error("Container:{} received invalid command:{}", containerId,
							commandType);
				}
			}catch(InterruptedException e){
				break;
			}
		}

		//publish COMMAND_WORKER_EXIT on commandSocket before exiting
		commandSocket.sendMore(topicName.getBytes());
		commandSocket.send(WorkerCommandHelper.serialize(Commands.CONTAINER_EXIT_COMMAND));
		System.out.println("Sent worker exit commmand");
	
		//container implementation specific cleanup
		cleanup();
		System.out.println("implementation specific cleanup done");

		//wait for all worker threads to exit
		try {
			for (String ebId: workers.keySet()) {
				System.out.println(ebId);
				Worker worker= workers.get(ebId);
				Thread workerThread= workerThreads.get(ebId);

				//only interrupt worker thread if is not connected to broker
				if(worker.connected()==Worker.STATE_DISCONNECTED){
					workerThread.interrupt();
				}
				//wait for worker thread to exit
				workerThread.join();
		
				//remove znode
				feSocket.send(FeRequestHelper.serialize(Commands.FE_DISCONNECT_REQUEST, 
						topicName,endpointType,containerId,ebId));
				resp=FeResponseHelper.deserialize(feSocket.recv());
				if(resp.code()==Frontend.RESPONSE_ERROR){
					logger.error("Container:{} got error:{} from Frontend",
							containerId,resp.msg());
				}
			}
		} catch (InterruptedException e) {
			logger.error("Container:{} caught exception:{}",containerId,e.getMessage());
		}
		cleanupZMQ();
		logger.info("Container:{} has exited",containerId);
	}
	
	private void exited(String ebId) throws InterruptedException{
		workers.remove(ebId);
		Thread workerThread=workerThreads.remove(ebId);
		workerThread.join();
		System.out.println("Worker connected to eb:"+ebId +" has exited");
		//request fe to remove znode for this worker
		feSocket.send(FeRequestHelper.serialize(Commands.FE_DISCONNECT_REQUEST, topicName,
				endpointType,containerId, ebId));
		FeResponse resp = FeResponseHelper.deserialize(feSocket.recv());
		if (resp.code() == Frontend.RESPONSE_ERROR) {
			logger.error("Container:{} got error:{} from Frontend", containerId, resp.msg());
		}
	}

	private void connect(String ebId,String topicConnector){
		feSocket.send(FeRequestHelper.serialize(Commands.FE_CONNECT_TO_EB_REQUEST, topicName,
				endpointType,containerId,ebId));
		FeResponse resp= FeResponseHelper.deserialize(feSocket.recv());
		if(resp.code()==Frontend.RESPONSE_OK){
			workerUuid++;
			Worker worker = createWorker(workerUuid,ebId,topicConnector);
			workers.put(ebId, worker);
			workerThreads.put(ebId, new Thread(worker));
			workerThreads.get(ebId).start();
		}
		if(resp.code()==Frontend.RESPONSE_ERROR){
			logger.error("Container:{} got error:{} from Frontend",containerId,resp.msg());
		}
	}
	
	private void disconnect(String ebId){
		commandSocket.sendMore(topicName.getBytes());
		commandSocket.send(WorkerCommandHelper.serialize(Commands.WORKER_EXIT_COMMAND,ebId));
	}
	
	private void disconnection(String ebId) throws InterruptedException{
		workers.remove(ebId);
		Thread workerThread=workerThreads.remove(ebId);
		workerThread.join();
		//request fe to remove znode for this worker
		feSocket.send(FeRequestHelper.serialize(Commands.FE_DISCONNECT_REQUEST, topicName,endpointType,
				containerId,ebId));
		FeResponse resp= FeResponseHelper.deserialize(feSocket.recv());
		if(resp.code()==Frontend.RESPONSE_ERROR){
			logger.error("Container:{} got error:{} from Frontend",
					containerId,resp.msg());
		}
		resp= queryFe();
		if(resp.code()==Frontend.RESPONSE_ERROR){
			logger.error("Container:{} got error:{} from Frontend",
					containerId,resp.msg());
			return;
		}
		if(resp.code()==Frontend.RESPONSE_OK){
			int numConnectors= resp.connectorsLength();
			for(int i=0;i<numConnectors;i++){
				TopicConnector connector= resp.connectors(i);
				String currEbId= connector.ebId();
				String topicConnector=String.format("%s,%d,%d,%d",connector.ebAddress(),
						connector.receivePort(),connector.sendPort(),connector.controlPort());
				if (!workers.containsKey(currEbId)) {
					workerUuid++;
					Worker newWorker = createWorker(workerUuid, currEbId, topicConnector);
					workers.put(currEbId, newWorker);
					workerThreads.put(currEbId, new Thread(newWorker));
					workerThreads.get(currEbId).start();
				}
			}
		}
	}

	private FeResponse queryFe() {
		// query FE for hosting EB's location
		feSocket.send(FeRequestHelper.serialize(Commands.FE_CONNECT_REQUEST, topicName, endpointType, containerId));
		return FeResponseHelper.deserialize(feSocket.recv());
	}	
	
	private void cleanupZMQ(){
		//ZMQ set linger to 0
		feSocket.setLinger(0);
		commandSocket.setLinger(0);
		queueSocket.setLinger(0);

		//close ZMQ sockets
		feSocket.close();
		commandSocket.close();
		queueSocket.close();

		//terminate ZMQ context
		context.term();
	}

	public String queueConnector(){
		return queueConnector;
	}
	
	public String commandConnector(){
		return commandConnector;
	}
	
	public abstract void initialize();

	public abstract void cleanup();
	
	public abstract void onConnected();
	
	public abstract Worker createWorker(int workerUuid, String ebId, String topicConnector);
}
