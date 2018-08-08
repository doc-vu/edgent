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
	
	private FeRequestHelper feRequestHelper;
	private WorkerCommandHelper workerCommandHelper;

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
    protected String experimentType;	
    protected int interval;
    protected String feAddress;
	
	protected Logger logger;

	public Container(String topicName,String endpointType,int id,String experimentType,int interval,String feAddress){
		logger= LogManager.getLogger(this.getClass().getName());

		//stash constructor arguments
		this.topicName=topicName;
		this.endpointType=endpointType;
		this.experimentType=experimentType;
		this.interval=interval;
		this.feAddress=feAddress;

		containerId=String.format("%s-%s-%s-%d",endpointType,topicName,
				UtilMethods.hostName(),id);
		workerUuid=0;
		workers=new HashMap<String,Worker>();
		workerThreads=new HashMap<String,Thread>();
		onConnectedCalled=false;
		
		feRequestHelper= new FeRequestHelper();
		workerCommandHelper=new WorkerCommandHelper();

		context=ZMQ.context(1);

		logger.debug("Container:{} initialized",containerId);
	}

	@Override
	public void run() {
		logger.info("Container:{} started", containerId);
		
		//initialize ZMQ sockets
		logger.info("Container:{} will initialize ZMQ sockets",containerId);
		if (!initializeZMQ()){
			return; 
		}

		//container implementation specific intialization
		initialize();
			
		
		//query FE to get hosting EB's location
		logger.info("Container:{} will query FE:{} to get hosting EB's location",
				containerId,feAddress);
		FeResponse feResp= queryFe();

		//start worker threads
		logger.info("Container:{} will start worker threads",containerId);
		startWorkers(feResp);

		//listener loop
		logger.info("Container:{} will start processing incoming commands",containerId );
		listen();

		//cleanup and exit
		exit();
		logger.info("Container:{} has exited",containerId);
	}


	private boolean initializeZMQ(){
		try{
			// initialize ZMQ.REQ socket to query FE
			feSocket = context.socket(ZMQ.REQ);
			feSocket.connect(String.format("tcp://%s:%d", feAddress, PortList.FE_LISTENER_PORT));

			// create and bind container's queue socket
			queueSocket = context.socket(ZMQ.PULL);
			int portNum=queueSocket.bindToRandomPort("tcp://localhost");
			queueConnector=String.format("tcp://localhost:%d", portNum);

			// create and bind container's command socket
			commandSocket = context.socket(ZMQ.PUB);
			portNum=commandSocket.bindToRandomPort("tcp://localhost");
			commandConnector=String.format("tcp://localhost:%d", portNum);
		
			logger.debug("Container:{} initialized with queueConnector:{}  commandConnector:{}", containerId, queueConnector,commandConnector);
			logger.info("Container:{} initialized ZMQ sockets", containerId);
			return true;
		}catch(Exception e){
			logger.error("Container:{} caught exception:{}",containerId,e.getMessage());
			cleanupZMQ();
			return false;
		}
	}

	private void cleanupZMQ(){
		//ZMQ set linger to 0
		if (feSocket!=null){
			feSocket.setLinger(0);
			feSocket.close();
		}
		
		if (commandSocket!=null){
			commandSocket.setLinger(0);
			commandSocket.close();
		}
		
		if (queueSocket !=null){
			queueSocket.setLinger(0);
			queueSocket.close();
		}

		//terminate ZMQ context
		context.term();
		logger.info("Container:{} closed ZMQ sockets and terminated context",containerId);
	}
	
	private FeResponse queryFe() {
		// query FE for hosting EB's location
		feSocket.send(feRequestHelper.serialize(Commands.FE_CONNECT_REQUEST, 
				topicName, endpointType, containerId,interval));

		FeResponse feResp= FeResponseHelper.deserialize(feSocket.recv());

		//If hosting EB could not be found, cleanup ZMQ sockets and exit
		if(feResp.code()==Frontend.FE_RESPONSE_CODE_ERROR){
			logger.error("Container:{} received error:{} from FE",containerId,feResp.msg());
			// publish CONTAINER_EXIT_COMMAND on commandSocket to signal all
			commandSocket.sendMore(topicName.getBytes());
			commandSocket.send(workerCommandHelper.serialize(Commands.CONTAINER_EXIT_COMMAND));
			logger.info("Container:{} sent CONTAINER_EXIT_COMMAND:{} to signal all threads to exit", containerId,
					Commands.CONTAINER_EXIT_COMMAND);
			cleanup();
			cleanupZMQ();
			return null;
		}
		return feResp;
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
	
	private void listen(){
		while (!Thread.currentThread().isInterrupted()) {
			try{
				logger.info("Container:{} will listen",containerId);
				ContainerCommand command= ContainerCommandHelper.deserialize(queueSocket.recv());
				int commandType=command.type();

				logger.info("Container:{} received:{}", containerId,
							Commands.CONTAINER_COMMANDS_MAP.get(commandType));

				//process container exit command
				if(commandType==Commands.CONTAINER_EXIT_COMMAND){
					break;
				}
				//process create worker command
				else if(commandType==Commands.CONTAINER_CREATE_WORKER_COMMAND){
					String targetContainerId= command.containerId();
					/* only create worker if our containerId equals ContainerCommand's containerId or 
					if this ContainerCommand is intended for all connected containers */
					if(targetContainerId.equals(FOR_ALL_CONTAINERS) || 
							targetContainerId.equals(endpointType)  ||
							targetContainerId.equals(containerId)){
						TopicConnector topicConnector=command.topicConnector();
						createWorker(topicConnector.ebId(),TopicConnectorHelper.toString(topicConnector));
					}
				}
				//delete worker command
				else if(commandType==Commands.CONTAINER_DELETE_WORKER_COMMAND){
					String targetContainerId= command.containerId();
					/* disconnect worker if our containerId equals ContainerCommand's containerId or 
					if this ContainerCommand is intended for all connected containers */
					if(targetContainerId.equals(FOR_ALL_CONTAINERS) ||
							targetContainerId.equals(endpointType)  ||
							targetContainerId.equals(containerId)){
						TopicConnector topicConnector= command.topicConnector();

						// send WORKER_EXIT_COMMAND for the worker connected to targetEb
						commandSocket.sendMore(topicName.getBytes());
						commandSocket.send(workerCommandHelper.serialize(Commands.WORKER_EXIT_COMMAND, topicConnector));
						logger.info("Container:{} sent WORKER_EXIT_COMMAND:{} for ebId:{}", containerId,
								Commands.WORKER_EXIT_COMMAND, topicConnector.ebId());
					}
				}
				//signal worker thread to exit immediately
				else if(commandType == Commands.CONTAINER_SIGNAL_WORKER_EXIT_IMMEDIATELY_COMMAND){
					String targetEbId= command.topicConnector().ebId();
					commandSocket.sendMore(topicName.getBytes());
					commandSocket.send(workerCommandHelper.serialize(Commands.WORKER_EXIT_IMMEDIATELY_COMMAND, command.topicConnector()));
					logger.info("Container:{} sent WORKER_EXIT_IMMEDIATELY_COMMAND:{} for ebId:{}", containerId,
							Commands.WORKER_EXIT_IMMEDIATELY_COMMAND, targetEbId);					
				}
				//worker connected command
				else if (commandType==Commands.CONTAINER_WORKER_CONNECTED_COMMAND) {
					feSocket.send(feRequestHelper.serialize(Commands.FE_CONNECT_TO_EB_REQUEST, topicName, endpointType,
							containerId, command.topicConnector().ebId(),experimentType,interval));
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
					TopicConnector topicConnector=command.topicConnector(); 
					removeWorker(topicConnector.ebId());
				}
				//worker exited command
				else if (commandType==Commands.CONTAINER_WORKER_EXITED_COMMAND) {
					String targetEbId=command.topicConnector().ebId(); 
					removeWorker(targetEbId);
					commandSocket.sendMore(topicName.getBytes());
					commandSocket.send(workerCommandHelper.serialize(Commands.WORKER_EXITED_COMMAND, command.topicConnector()));
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
	}

	private void exit(){
		//publish CONTAINER_EXIT_COMMAND on commandSocket to signal all threads to exit
		commandSocket.sendMore(topicName.getBytes());
		commandSocket.send(workerCommandHelper.serialize(Commands.CONTAINER_EXIT_COMMAND));
		logger.info("Container:{} sent CONTAINER_EXIT_COMMAND:{} to signal all threads to exit",
				containerId,Commands.CONTAINER_EXIT_COMMAND);
	

		//wait for all worker threads to exit
		try {
			for (String ebId: workers.keySet()) {
				Worker worker= workers.get(ebId);
				Thread workerThread= workerThreads.get(ebId);

				//only interrupt worker thread if is not connected to broker
				if(worker.connected()==Worker.WORKER_STATE_DISCONNECTED_SAFE_TO_INTERRUPT){
					workerThread.interrupt();
				}
				logger.info("Container:{} will wait for worker thread to exit",containerId);
				//wait for worker thread to exit
				workerThread.join();
				logger.info("Container:{} worker thread has exited",containerId);
		
				//remove znode for this worker thread 
				logger.info("Container:{} will request FE to remove worker's znode",containerId);
				feSocket.send(feRequestHelper.serialize(Commands.FE_DISCONNECT_REQUEST, 
						topicName,endpointType,containerId,ebId,experimentType,interval));
				FeResponse feResp=FeResponseHelper.deserialize(feSocket.recv());

				logger.info("Container:{} received response code:{} from FE",containerId,feResp.code());
				if(feResp.code()==Frontend.FE_RESPONSE_CODE_ERROR){
					logger.error("Container:{} got error:{} from Frontend",
							containerId,feResp.msg());
				}
			}
		} catch (InterruptedException e) {
			logger.error("Container:{} caught exception:{}",containerId,e.getMessage());
		}
		logger.info("Container:{} all worker threads have exited",containerId);

		//perform container implementation specific cleanup
		cleanup();
		//close sockets and terminate context
		cleanupZMQ();
	}

	private void createWorker(String ebId, String topicConnector){
		workerUuid++;
		//create worker
		Worker worker=instantiateWorker(context,workerUuid,ebId,topicConnector);
		if(worker!=null){
			workers.put(ebId, worker);
			//start worker thread
			workerThreads.put(ebId, new Thread(worker));
			workerThreads.get(ebId).start();
			logger.info("Container:{} started worker thread for eb:{} and topic connector:{}",
					containerId,ebId,topicConnector);
		}
	}
	
	private void removeWorker(String ebId) throws InterruptedException{
		//remove worker and worker thread instance 
		workers.remove(ebId);
		Thread workerThread = workerThreads.remove(ebId);
		//wait for worker thread to exit
		workerThread.join();
		logger.info("Container:{} worker for ebId:{} has exited", containerId, ebId);

		//request fe to remove znode for this worker
		feSocket.send(feRequestHelper.serialize(Commands.FE_DISCONNECT_REQUEST, 
				topicName,endpointType,containerId,ebId,experimentType,interval));
		FeResponse resp= FeResponseHelper.deserialize(feSocket.recv());
		if(resp.code()==Frontend.FE_RESPONSE_CODE_OK){
			logger.info("Container:{} removed znode for worker connected to eb:{}",containerId,ebId);
		}
		if(resp.code()==Frontend.FE_RESPONSE_CODE_ERROR){
			logger.info("Container:{} got error:{} from Frontend",
					containerId,resp.msg());
		}
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
	public abstract Worker instantiateWorker(ZMQ.Context context, int workerUuid, String ebId, String topicConnector);
}
