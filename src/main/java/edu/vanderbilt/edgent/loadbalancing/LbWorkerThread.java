package edu.vanderbilt.edgent.loadbalancing;

import java.util.List;
import org.apache.curator.framework.CuratorFramework;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.zeromq.ZMQ;

public class LbWorkerThread implements Runnable {
	//ZMQ context 
	private ZMQ.Context context;
	//ZMQ PULL socket to receive topic creation requests
	private ZMQ.Socket listenerSocket;
	//ZMQ SUB socket to receive control messages
	private ZMQ.Socket controlSocket;

	//ZMQ Poller to listen to both listener and control socket
	private ZMQ.Poller poller;

	//CuratorClient to connect to ZK
	private CuratorFramework client;

	private String workerId;
	private Logger logger;

	public LbWorkerThread(ZMQ.Context context,CuratorFramework client){
		this.context=context;
		this.client=client;
		logger=LogManager.getLogger(this.getClass().getSimpleName());
	}

	@Override
	public void run() {
		workerId= Thread.currentThread().getName();
		logger.debug("WorkerThread:{} started",workerId);

		//Create ZMQ listener socket to get topic creation requests 
		listenerSocket=context.socket(ZMQ.PULL);
		listenerSocket.connect(LoadBalancer.INPROC_CONNECTOR);

		//create ZMQ subscriber socket to receive control messages
		controlSocket=context.socket(ZMQ.SUB);
		controlSocket.connect(String.format("tcp://127.0.0.1:%d",
				LoadBalancer.CONTROL_PORT));
		controlSocket.subscribe(LoadBalancer.CONTROL_TOPIC.getBytes());
	
		//initialize ZMQ poller
		poller=context.poller(2);
		poller.register(listenerSocket,ZMQ.Poller.POLLIN);
		poller.register(controlSocket,ZMQ.Poller.POLLIN);
	
		while(true){
			// block until either listener or control socket have a message
			poller.poll(-1);

			if (poller.pollin(0)) {
				// process topic creation requests
				String topic = listenerSocket.recvStr();
				logger.info("WorkerThread:{} received topic creation request for topic:{}",
						workerId, topic);
				create(topic);
			}
			if (poller.pollin(1)) {
				// process control messages
				String[] args = controlSocket.recvStr().split(" ");
				if (args[1].equals(LoadBalancer.SHUTDOWN_CONTROL_MSG)) {
					logger.info("WorkerThread:{} received control msg:{}",workerId,args[1]);
					break;
				}
			}
		}
		//set linger to 0 
		listenerSocket.setLinger(0);
		controlSocket.setLinger(0);
		//close sockets before exiting
		listenerSocket.close();
		controlSocket.close();
		logger.info("WorkerThread:{} exited",workerId);
	}
	
	private void create(String topic){
		try{
			//create topic znode under /topics
			client.create()
				.forPath(String.format("/topics/%s",topic));
			logger.info("WorkerThread:{} created topic znode:/topics/{}",workerId,topic);

			//get a list of EBs in the system
			List<String> ebs= client.getChildren().forPath("/eb");
		
			//select least loaded eb to host the new topic
			String selectedEb= selectEb(ebs);
			if(selectedEb!=null){
				logger.debug("WorkerThread:{} selected EB:{} for hosting topic:{}", 
						workerId, selectedEb, topic);

				// create topic znode under selected EB's znode
				client.create().creatingParentsIfNeeded().
					forPath(String.format("/eb/%s/%s/pub", selectedEb, topic));
				client.create().creatingParentsIfNeeded().
					forPath(String.format("/eb/%s/%s/sub", selectedEb, topic));
				logger.info("WorkerThread:{} assigned topic:{} to EB:{}", workerId, topic, selectedEb);
			}else{
				client.delete().forPath(String.format("/topics/%s", topic));
				logger.error("WorkerThread:{} topic:{} cannot be hosted. Deleted topic:{}",
						workerId,topic,topic);
			}
		}catch(KeeperException e){
			if(e.code().equals(Code.NODEEXISTS)){
				logger.info("WorkerThread:{} already topic:{} exists",workerId,topic);
			}
			logger.error("WorkerThread:{} caught exception:{}",workerId,e.getMessage());

		}catch(Exception e){
			logger.error("WorkerThread:{} caught exception:{}",workerId,e.getMessage());
		}
	}

	/*
	 * TODO: Currently, an EB is randomly selected to host 
	 * the new topic. 
	 */
	private String selectEb(List<String> ebs){
		if(ebs.isEmpty()){
			return null;
		}
		int selectedBroker = (int) (Math.random() * ebs.size());
		return ebs.get(selectedBroker);
	}

}