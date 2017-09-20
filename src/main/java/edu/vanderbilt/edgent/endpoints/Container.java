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
import edu.vanderbilt.edgent.types.TopicConnectorHelper;
import edu.vanderbilt.edgent.types.WorkerCommandHelper;
import edu.vanderbilt.edgent.util.Commands;
import edu.vanderbilt.edgent.util.PortList;
import edu.vanderbilt.edgent.util.UtilMethods;

public abstract class Container implements Runnable{
	//Endpoint types
  	public static final String ENDPOINT_TYPE_SUB="sub";
  	public static final String ENDPOINT_TYPE_PUB="pub";
  	//Matching string to denote that this LB command is to be executed by all containers
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
	
	//Map to hold references to Workers 
	protected HashMap<String,Worker> workers;
	//Map to hold references to Worker threads
	protected HashMap<String,Thread> workerThreads;

	/* onConnected should only be called once when all
	 * the initial number of worker threads have joined.
	 * flag to determine whether onConnected has been called or not.
	 * Set to true when onConnected is called for the first time.
	 * Subsequent calls to onConnected will return.
	 */
	private boolean onConnectedCalled;
	//counter to accord unique/increasing value of workerId to worker threads
	private int workerUuid;

	//type of container endpoint
	protected String endpointType;
	//topic name
	protected String topicName;

	protected String containerId;
	protected Logger logger;

	public Container( String topicName,String endpointType,int id){
		logger= LogManager.getLogger(this.getClass().getSimpleName());
		//stash constructor arguments
		this.topicName=topicName;
		this.endpointType=endpointType;

		containerId=String.format("%s-%s-%s-%d",endpointType,topicName,
				UtilMethods.hostName(),id);
		workerUuid=0;
		workers=new HashMap<String,Worker>();
		workerThreads=new HashMap<String,Thread>();
		onConnectedCalled=false;

		context=ZMQ.context(1);

		if(endpointType.equals(ENDPOINT_TYPE_SUB)){
			commandConnector = String.format("tcp://*:%d", (PortList.SUBSCRIBER_COMMAND_BASE_PORT_NUM + id));
			queueConnector = String.format("tcp://*:%d", (PortList.SUBSCRIBER_QUEUE_BASE_PORT_NUM + id));
		}
		if(endpointType.equals(ENDPOINT_TYPE_PUB)){
			commandConnector = String.format("tcp://*:%d", (PortList.PUBLISHER_COMMAND_BASE_PORT_NUM + id));
			queueConnector = String.format("tcp://*:%d", (PortList.PUBLISHER_QUEUE_BASE_PORT_NUM + id));
		}
		logger.debug("Container:{} initialized",containerId);
	}

	@Override
	public void run() {
		logger.info("Container:{} started", containerId);
		//acquire this region's known FE endpoint
		String feAddress= Frontend.FE_LOCATIONS.get(UtilMethods.regionId());

		if(feAddress==null){
			logger.error("Container:{} could not locate it's region's FE(Region Id:{}). Exiting.",
					containerId,UtilMethods.regionId());
			context.term();
			return;
		}

		//initialize ZMQ.REQ socket to query FE
		feSocket=context.socket(ZMQ.REQ);
		feSocket.connect(String.format("tcp://%s:%d",feAddress,PortList.FE_LISTENER_PORT));

		//create and bind container's queue socket 
		queueSocket= context.socket(ZMQ.PULL);
		queueSocket.bind(queueConnector);

		//create and bind container's command socket
		commandSocket= context.socket(ZMQ.PUB);
		commandSocket.bind(commandConnector);
		
		logger.debug("Container:{} initialized ZMQ sockets",containerId);

		//container implementation specific intialization
		initialize();
		
		logger.info("Container:{} will query FE:{} to get hosting EB's location",containerId,
				feAddress);
		//query FE to get hosting EB's location
		FeResponse feResp= queryFe();

		//If hosting EB could not be found, cleanup ZMQ sockets and exit
		if(feResp.code()==Frontend.FE_RESPONSE_CODE_ERROR){
			logger.error("Container:{} received error:{} from FE",containerId,feResp.msg());
			cleanupZMQ();
			return;
		}
		
		//start worker threads
		logger.info("Container:{} will start worker threads",containerId);
		startWorkers(feResp);

		logger.info("Container:{} will start processing incoming commands",containerId );
		while (!Thread.currentThread().isInterrupted()) {
			try{
				logger.info("Container:{} will listen",containerId);
				ContainerCommand command= ContainerCommandHelper.deserialize(queueSocket.recv());
				int commandType=command.type();

				//process container exit command
				if(commandType==Commands.CONTAINER_EXIT_COMMAND){
					logger.info("Container:{} received CONTAINER_EXIT_COMMAND:{}. Will exit listener loop.", containerId,
							Commands.CONTAINER_EXIT_COMMAND);
					break;
				}
				//process create worker command
				else if(commandType==Commands.CONTAINER_CREATE_WORKER_COMMAND){
					logger.info("Container:{} received CONTAINER_CREATE_WORKER_COMMAND:{}", containerId,
							Commands.CONTAINER_CREATE_WORKER_COMMAND);
					String targetContainerId= command.containerId();
					/* only create worker if our containerId equals ContainerCommand's containerId or 
					if this ContainerCommand is intended for all connected containers */
					if(targetContainerId.equals(FOR_ALL_CONTAINERS) || 
							targetContainerId.equals(endpointType)  ||
							targetContainerId.equals(containerId)){
						TopicConnector topicConnector=command.topicConnector();
						processCreateWorkerCommand(topicConnector.ebId(),TopicConnectorHelper.toString(topicConnector));
					}
				}
				//delete worker command
				else if(commandType==Commands.CONTAINER_DELETE_WORKER_COMMAND){
					logger.info("Container:{} received CONTAINER_DELETE_WORKER_COMMAND:{}" , containerId,
							Commands.CONTAINER_DELETE_WORKER_COMMAND);
					String targetContainerId= command.containerId();
					/* disconnect worker if our containerId equals ContainerCommand's containerId or 
					if this ContainerCommand is intended for all connected containers */
					if(targetContainerId.equals(FOR_ALL_CONTAINERS) ||
							targetContainerId.equals(endpointType)  ||
							targetContainerId.equals(containerId)){
						TopicConnector topicConnector= command.topicConnector();
						processDeleteWorkerCommand(topicConnector);
					}
				}
				//signal worker thread to exit immediately
				else if(commandType == Commands.CONTAINER_SIGNAL_WORKER_EXIT_IMMEDIATELY_COMMAND){
					String targetEbId= command.topicConnector().ebId();
					logger.info("Container:{} received CONTAINER_SIGNAL_WORKER_EXIT_IMMEDIATELY_COMMAND:{} for eb:{}", containerId,
							Commands.CONTAINER_SIGNAL_WORKER_EXIT_IMMEDIATELY_COMMAND, targetEbId);					
					commandSocket.sendMore(topicName.getBytes());
					commandSocket.send(WorkerCommandHelper.serialize(Commands.WORKER_EXIT_IMMEDIATELY_COMMAND, command.topicConnector()));
					logger.info("Container:{} sent WORKER_EXIT_IMMEDIATELY_COMMAND:{} for ebId:{}", containerId,
							Commands.WORKER_EXIT_IMMEDIATELY_COMMAND, targetEbId);					
				}
				//worker connected command
				else if (commandType==Commands.CONTAINER_WORKER_CONNECTED_COMMAND) {
					logger.info("Container:{} received CONTAINER_WORKER_CONNECTED_COMMAND:{}", containerId,
							Commands.CONTAINER_WORKER_CONNECTED_COMMAND);

					feSocket.send(FeRequestHelper.serialize(Commands.FE_CONNECT_TO_EB_REQUEST, topicName, endpointType,
							containerId, command.topicConnector().ebId()));
					FeResponse resp = FeResponseHelper.deserialize(feSocket.recv());
					if (resp.code() == Frontend.FE_RESPONSE_CODE_OK) {
						logger.info("Container:{} created its znode under ebId:{}",containerId,command.topicConnector().ebId());
					}
					if (resp.code() == Frontend.FE_RESPONSE_CODE_ERROR) {
						logger.error("Container:{} got error:{} from Frontend", containerId, resp.msg());
					}
					//onConnected callback is only called once when all the workers are in the connected state
					if(!onConnectedCalled){
						// check if all workers are in connected state
						if (workers.values().stream().allMatch(w -> w.connected() == Worker.WORKER_STATE_CONNECTED)) {
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
					onWorkerDisconnected(topicConnector.ebId());
				}
				//worker exited command
				else if (commandType==Commands.CONTAINER_WORKER_EXITED_COMMAND) {
					logger.info("Container:{} received CONTAINER_WORKER_EXITED_COMMAND:{}", containerId,
							Commands.CONTAINER_WORKER_EXITED_COMMAND);
					String targetEbId=command.topicConnector().ebId(); 
					onWorkerExited(targetEbId);
					commandSocket.sendMore(topicName.getBytes());
					commandSocket.send(WorkerCommandHelper.serialize(Commands.WORKER_EXITED_COMMAND, command.topicConnector()));
					logger.info("Container:{} sent WORKER_EXITED_COMMAND:{} for ebId:{}", containerId,
							Commands.WORKER_EXITED_COMMAND, targetEbId);					
					
				}else{
					logger.error("Container:{} received invalid command:{}", containerId,
							commandType);
				}
			}catch(InterruptedException e){
				logger.error("Container:{} caught exception:{}",containerId,e.getMessage());
				break;
			}
		}

		//publish CONTAINER_EXIT_COMMAND on commandSocket to signal all threads to exit
		commandSocket.sendMore(topicName.getBytes());
		commandSocket.send(WorkerCommandHelper.serialize(Commands.CONTAINER_EXIT_COMMAND));
		logger.info("Container:{} sent CONTAINER_EXIT_COMMAND:{} to signal all threads to exit",
				containerId,Commands.CONTAINER_EXIT_COMMAND);
	
		//perform container implementation specific cleanup
		cleanup();

		//wait for all worker threads to exit
		try {
			for (String ebId: workers.keySet()) {
				Worker worker= workers.get(ebId);
				Thread workerThread= workerThreads.get(ebId);

				//only interrupt worker thread if is not connected to broker
				if(worker.connected()==Worker.WORKER_STATE_DISCONNECTED){
					workerThread.interrupt();
				}
				//wait for worker thread to exit
				workerThread.join();
		
				//remove znode for this worker thread 
				feSocket.send(FeRequestHelper.serialize(Commands.FE_DISCONNECT_REQUEST, 
						topicName,endpointType,containerId,ebId));
				feResp=FeResponseHelper.deserialize(feSocket.recv());
				if(feResp.code()==Frontend.FE_RESPONSE_CODE_ERROR){
					logger.error("Container:{} got error:{} from Frontend",
							containerId,feResp.msg());
				}
			}
		} catch (InterruptedException e) {
			logger.error("Container:{} caught exception:{}",containerId,e.getMessage());
		}
		logger.info("Container:{} all worker threads have exited",containerId);
		cleanupZMQ();
		logger.info("Container:{} has exited",containerId);
	}

	private FeResponse queryFe() {
		// query FE for hosting EB's location
		feSocket.send(FeRequestHelper.serialize(Commands.FE_CONNECT_REQUEST, 
				topicName, endpointType, containerId));
		return FeResponseHelper.deserialize(feSocket.recv());
	}	
	
	private void startWorkers(FeResponse feResp){
		//create a worker thread for each topicConnector received in FE's response
		for(int i=0;i<feResp.connectorsLength();i++){
			TopicConnector connector= feResp.connectors(i);
			String ebId= connector.ebId();
			String connectorString= TopicConnectorHelper.toString(connector);
			createWorker(ebId,connectorString);
		}
	}

	private void processCreateWorkerCommand(String newEbId,String newTopicConnector){
		createWorker(newEbId,newTopicConnector);
	}
	
	private void processDeleteWorkerCommand(TopicConnector connector){
		//send WORKER_EXIT_COMMAND for the worker connected to targetEbId
		commandSocket.sendMore(topicName.getBytes());
		commandSocket.send(WorkerCommandHelper.serialize(Commands.WORKER_EXIT_COMMAND,connector));
		logger.info("Container:{} sent WORKER_EXIT_COMMAND:{} for ebId:{}",containerId,
				Commands.WORKER_EXIT_COMMAND,connector.ebId());
	}
	
	private void onWorkerDisconnected(String ebId) throws InterruptedException{
		//remove worker and worker thread for ebId
		removeWorker(ebId);
		
		//query FE to get locators for topic of interest
		FeResponse resp= queryFe();
		if(resp.code()==Frontend.FE_RESPONSE_CODE_ERROR){
			logger.error("Container:{} got error:{} from Frontend",
					containerId,resp.msg());
			return;
		}
		if(resp.code()==Frontend.FE_RESPONSE_CODE_OK){
			int numConnectors= resp.connectorsLength();
			for(int i=0;i<numConnectors;i++){
				TopicConnector connector= resp.connectors(i);
				String currEbId= connector.ebId();
				String topicConnector= TopicConnectorHelper.toString(connector);
				//only start a worker for ebId for which worker does not exist
				if (!workers.containsKey(currEbId)) {
					createWorker(currEbId,topicConnector);
				}
			}
		}
	}

	private void onWorkerExited(String ebId) throws InterruptedException{
		//remove worker and worker thread for ebId
		removeWorker(ebId);
	}
	
	private void createWorker(String ebId, String topicConnector){
		workerUuid++;
		//create worker
		Worker worker=instantiateWorker(workerUuid,ebId,topicConnector);
		workers.put(ebId, worker);
		//start worker thread
		workerThreads.put(ebId, new Thread(worker));
		workerThreads.get(ebId).start();
		logger.info("Container:{} started worker thread for eb:{} and topic connector:{}",
					containerId,ebId,topicConnector);
	}
	
	private void removeWorker(String ebId) throws InterruptedException{
		//remove worker and worker thread instance 
		workers.remove(ebId);
		Thread workerThread = workerThreads.remove(ebId);
		//wait for worker thread to exit
		workerThread.join();
		logger.info("Container:{} worker for ebId:{} has exited", containerId, ebId);

		//request fe to remove znode for this worker
		feSocket.send(FeRequestHelper.serialize(Commands.FE_DISCONNECT_REQUEST, 
				topicName,endpointType,containerId,ebId));
		FeResponse resp= FeResponseHelper.deserialize(feSocket.recv());
		if(resp.code()==Frontend.FE_RESPONSE_CODE_OK){
			logger.info("Container:{} removed znode for worker connected to eb:{}",containerId,ebId);
		}
		if(resp.code()==Frontend.FE_RESPONSE_CODE_ERROR){
			logger.info("Container:{} got error:{} from Frontend",
					containerId,resp.msg());
		}
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
		logger.info("Container:{} closed ZMQ sockets and terminated context",containerId);
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
	public abstract Worker instantiateWorker(int workerUuid, String ebId, String topicConnector);
}
