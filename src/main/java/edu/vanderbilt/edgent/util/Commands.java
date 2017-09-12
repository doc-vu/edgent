package edu.vanderbilt.edgent.util;

public class Commands {
	// FE Request Commands
	public static final int FE_CONNECT_REQUEST = 0;
	public static final int FE_CONNECT_TO_EB_REQUEST = 1;
	public static final int FE_DISCONNECT_REQUEST = 2;
	
	
	//Type of commands processed by Container
	public static final int CONTAINER_EXIT_COMMAND=3;
	public static final int CONTAINER_CREATE_WORKER_COMMAND=4;
	public static final int CONTAINER_DELETE_WORKER_COMMAND=5;
	public static final int CONTAINER_WORKER_CONNECTED_COMMAND=6;
	public static final int CONTAINER_WORKER_DISCONNECTED_COMMAND=7;
	public static final int CONTAINER_WORKER_EXITED_COMMAND=8;
	
	// Worker thread commands
	public static final int WORKER_EXIT_COMMAND=9;
	

}
