package edu.vanderbilt.edgent.util;

import java.util.HashMap;

public class Commands {
	//commands processed by FEWorker threads
	public static final int FE_CONNECT_REQUEST = 0;
	public static final int FE_CONNECT_TO_EB_REQUEST = 1;
	public static final int FE_DISCONNECT_REQUEST = 2;
	
	
	//commands processed by Containers
	public static final int CONTAINER_EXIT_COMMAND=3;
	public static final int CONTAINER_CREATE_WORKER_COMMAND=4;
	public static final int CONTAINER_DELETE_WORKER_COMMAND=5;
	public static final int CONTAINER_SIGNAL_WORKER_EXIT_IMMEDIATELY_COMMAND=6;
	public static final int CONTAINER_WORKER_CONNECTED_COMMAND=7;
	public static final int CONTAINER_WORKER_DISCONNECTED_COMMAND=8;
	public static final int CONTAINER_WORKER_EXITED_COMMAND=9;
	
	public static final HashMap<Integer,String> CONTAINER_COMMANDS_MAP=new HashMap<Integer,String>();
	static{
		CONTAINER_COMMANDS_MAP.put(CONTAINER_EXIT_COMMAND, "CONTAINER_EXIT_COMMAND");
		CONTAINER_COMMANDS_MAP.put(CONTAINER_CREATE_WORKER_COMMAND, "CONTAINER_CREATE_WORKER_COMMAND");
		CONTAINER_COMMANDS_MAP.put(CONTAINER_DELETE_WORKER_COMMAND, "CONTAINER_DELETE_WORKER_COMMAND");
		CONTAINER_COMMANDS_MAP.put(CONTAINER_SIGNAL_WORKER_EXIT_IMMEDIATELY_COMMAND, "CONTAINER_SIGNAL_WORKER_EXIT_IMMEDIATELY_COMMAND");
		CONTAINER_COMMANDS_MAP.put(CONTAINER_WORKER_CONNECTED_COMMAND, "CONTAINER_WORKER_CONNECTED_COMMAND");
		CONTAINER_COMMANDS_MAP.put(CONTAINER_WORKER_DISCONNECTED_COMMAND, "CONTAINER_WORKER_DISCONNECTED_COMMAND");
		CONTAINER_COMMANDS_MAP.put(CONTAINER_WORKER_EXITED_COMMAND, "CONTAINER_WORKER_EXITED_COMMAND");
	}
		

	
	//commands processed by Worker threads 
	public static final int WORKER_EXIT_COMMAND=10;
	public static final int WORKER_EXIT_IMMEDIATELY_COMMAND=11;
	public static final int WORKER_EXITED_COMMAND=12;

	//commands processed by Topic threads
	public static final int TOPIC_DELETE_COMMAND=13;
	public static final int TOPIC_LB_COMMAND=14;
	
	//Type of commands processed by EB
	public static final String EB_TOPIC_DELETE_COMMAND="DELETE_TOPIC";
	public static final String EB_TOPIC_CREATE_COMMAND="CREATE_TOPIC";
	public static final String EB_TOPIC_LB_COMMAND="LB_TOPIC";
	public static final String EB_EXIT_COMMAND="EXIT_EB";
	
	//Type of commands processed by LBListener
	public static final String LB_LISTENER_EXIT_COMMAND="exit";

}
