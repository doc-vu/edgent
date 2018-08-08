package edu.vanderbilt.edgent.endpoints;

import org.zeromq.ZMQ;
import edu.vanderbilt.edgent.endpoints.publisher.Publisher;
import edu.vanderbilt.edgent.endpoints.subscriber.Subscriber;
import edu.vanderbilt.edgent.types.ContainerCommandHelper;
import edu.vanderbilt.edgent.util.Commands;
import edu.vanderbilt.edgent.util.UtilMethods;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.*;

public class EndpointManager {
    private static final String GLOBAL_CONTROL_TOPIC_NAME="global_control";
    private static final String COMMAND_START="start";
    private static final String COMMAND_KILL="kill";
    private static final String COMMAND_EXIT="exit";

    //EndpointManager id and hostname
	private int id;
	private String mqConnector;
	private String hostname;

	//ZMQ context and sockets for receiving commands to start/kill endpoint container 
	private ZMQ.Context context;
	private ZMQ.Socket listener;
    private	ContainerCommandHelper containerCommandHelper;

    //References to currently executing container endpoint and thread
	private Container endpoint;
	private Thread endpointThread;

	private Logger logger;

	public EndpointManager(int id,String mqConnector){
		logger= LogManager.getLogger(this.getClass().getName());
		this.id=id;
		this.mqConnector=mqConnector;
		this.endpoint=null;
		this.endpointThread=null;
		String hostname=UtilMethods.hostName();
		if(UtilMethods.hostname_alias.containsKey(hostname)){
			this.hostname=UtilMethods.hostname_alias.get(hostname);
		}else{
			this.hostname=hostname;
		}
		containerCommandHelper= new ContainerCommandHelper();
		logger.info("EndpointManager:{} on host:{} started",this.id,this.hostname);
	}
	
	public void listen(){
		try{
			initialize();
			while (true) {
				logger.info("EndpointManager:{} on host:{} will listen for command", id, hostname);
				String msgStr = listener.recvStr();
				String[] parts = msgStr.split(" ");
				String topicName = parts[0];
				String msg = msgStr.substring(topicName.length() + 1);

				logger.info("EndpointManager:{} on host:{} received:\ntopic:{}\ncommand:{}\n", id, hostname, topicName,
						msg);

				// Either host specific commands to start/kill container
				// end-points are received
				if (topicName.equals(hostname)) {
					JSONObject obj = new JSONObject(msg);
					String command = obj.getString("command");

					if (command.equals(COMMAND_START)) {// Start end-point
						String endpoint = obj.getString("endpoint");
						if (endpoint.equals("pub")) {
							start_publisher(obj);
						} else if (endpoint.equals("sub")) {
							start_subscriber(obj);
						} else {
							logger.error("EndpointManager:{} on host:{} endpoint:{} not recognized", id, hostname,
									endpoint);
						}
					} else if (command.equals(COMMAND_KILL)) {// kill end-point
						kill();
					} else if (command.equals(COMMAND_EXIT)) {// exit listener
																// loop
						break;
					} else {
						logger.error("EndpointManager:{} on host:{} command:{} not recognized", id, hostname, msg);
					}

				}
				// Else commands to start/kill container endpoints are sent to
				// all endpoint managers running on all host machines
				else if (topicName.equals(GLOBAL_CONTROL_TOPIC_NAME)) {
					if (msg.equals("kill")) {
						kill();
					} else if (msg.equals("exit")) {
						break;
					} else {
						logger.error("EndpointManager:{} on host:{} command:{} not recognized", id, hostname, msg);
					}

				} else {
					logger.error("EndpointManager:{} on host:{} topic name:{} not recognized", id, hostname, topicName);
				}
			}
			cleanup();
			logger.info("EndpointManager:{} on host:{} exited", id, hostname);
		}catch(Exception e){
			logger.error("EndpointManager:{} on host:{} caught exception:{}",id,hostname,e.getMessage());
		}
	}
	
	private void initialize(){
		//create ZMQ context
		context= ZMQ.context(1);

		//create socket to receive control commands
		listener=context.socket(ZMQ.SUB);
		listener.setHWM(0);
		listener.connect(mqConnector);

		//subscribe to this hostname's topic 
		listener.subscribe(this.hostname.getBytes());
		//subscribe to control topic
		listener.subscribe(GLOBAL_CONTROL_TOPIC_NAME.getBytes());
		
		logger.info("EndpointManager:{} on host:{} initialized ZMQ context and SUB socket",id,hostname);
	}

	private void start_publisher(JSONObject obj ){
		if(endpoint==null){
			JSONArray topic_descriptions= obj.getJSONArray("topic_descriptions");
			
			//Extract topic_descriptor meant for this end-point manager
			if (topic_descriptions.length()<id){
				return;
			}
			String tdesc=(String) topic_descriptions.get(id-1);
			String[] tdesc_parts= tdesc.split(":");
			//Extract topic_descriptor parts
			String topic_name=tdesc_parts[0];
			int publication_rate=Integer.parseInt(tdesc_parts[2]);
			int sample_count=Integer.parseInt(tdesc_parts[3]);
			int payload_size=Integer.parseInt(tdesc_parts[4]);
			int processing_interval=Integer.parseInt(tdesc_parts[5]);
			int send_interval= 1000 /publication_rate;

			endpoint=new Publisher(topic_name,
					id,
					sample_count,
					send_interval,
					payload_size,
					obj.getString("zk_connector"),
					obj.getString("experiment_type"),
					1,//send data
					processing_interval,
					obj.getString("fe_address"));
			
			endpointThread=new Thread(endpoint);
			endpointThread.start();
			logger.info("EndpointManager:{} on host:{} started publisher endpoint for topic:{}",id,hostname,topic_name);
		}else{
			logger.error("EndpointManager:{} on host:{} a publisher endpoint is already running", id,hostname);
		}
	}

	private void start_subscriber(JSONObject obj){
		if(endpoint==null){
			JSONArray topic_descriptions=obj.getJSONArray("topic_descriptions");

			//Extract topic_descriptor meant for this end-point manager
			if (topic_descriptions.length()<id){
				return;
			}
			String tdesc= (String)topic_descriptions.get(id-1);
			String[] tdesc_parts= tdesc.split(":");
			//Extract topic_descriptor parts
			String topic_name= tdesc_parts[0];
			int sample_count=Integer.parseInt(tdesc_parts[2]);
			int processing_interval=Integer.parseInt(tdesc_parts[3]);

			endpoint=new Subscriber(topic_name,
					id,
					sample_count,
					obj.getInt("run_id"),
					obj.getString("log_dir"),
					true,//log latency
					obj.getString("experiment_type"),
					processing_interval,
					obj.getString("fe_address"));

			endpointThread= new Thread(endpoint);
			endpointThread.start();
			logger.info("EndpointManager:{} on host:{} started subscriber endpoint for topic:{}",id,hostname,topic_name);
		}else{
			logger.error("EndpointManager:{} on host:{} a subscriber endpoint is already running", id,hostname);
		}
	}
	
	private void kill(){
		logger.info("EndpointManager:{} on host:{} kill method called",id,hostname);

		if(endpoint!=null){
			logger.info("EndpointManager:{} on host:{} will kill existing endpoint container",id,hostname);
			ZMQ.Socket pushSocket = context.socket(ZMQ.PUSH);
			pushSocket.connect(endpoint.queueConnector());
			// send CONTAINER_EXIT_COMMAND
			pushSocket.send(containerCommandHelper.serialize(Commands.CONTAINER_EXIT_COMMAND));
			// wait for endpointThread to exit
			try {
				if(endpointThread!=null){
					logger.info("EndpointManager:{} on host:{} will wait for endpoint thread to exit",id,hostname);
					endpointThread.join();
				}
				endpoint=null;
				endpointThread=null;
				logger.info("EndpointManager:{} on host:{} reset endpoint reference",id,hostname);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			// cleanup ZMQ
			pushSocket.setLinger(0);
			pushSocket.close();
		}
		logger.info("EndpointManager:{} on host:{} there is no existing container endpoint running",id,hostname);
	}
	
	private void cleanup(){
		//set linger to 0
		listener.setLinger(0);
		//close zmq socket and context
		listener.close();
		context.term();
	}
	
	public static void main(String args[]){
		if (args.length<2){
			System.out.println("EndpointManager id mqConnector");
			return;
		}
		int id=Integer.parseInt(args[0]);
		String mqConnector=args[1];
		EndpointManager mgr= new EndpointManager(id,mqConnector);
		mgr.listen();
	}

}
