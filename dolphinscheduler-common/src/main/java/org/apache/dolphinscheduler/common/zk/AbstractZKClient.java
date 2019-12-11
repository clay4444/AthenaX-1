/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dolphinscheduler.common.zk;

import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.IStoppable;
import org.apache.dolphinscheduler.common.enums.ZKNodeType;
import org.apache.dolphinscheduler.common.model.Server;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.common.utils.OSUtils;
import org.apache.dolphinscheduler.common.utils.ResInfo;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.dolphinscheduler.common.Constants.*;


/**
 * abstract zookeeper client
 *
 * 抽象的自定义 zkClient，封装着真正的 CuratorFramework  zkClient
 */
public abstract class AbstractZKClient {

	private static final Logger logger = LoggerFactory.getLogger(AbstractZKClient.class);

	/**
	 *  load configuration file
	 */
	protected static Configuration conf;

	protected  CuratorFramework zkClient = null;

	/**
	 * server stop or not
	 */
	protected IStoppable stoppable = null;


	static {
		try {
			conf = new PropertiesConfiguration(Constants.ZOOKEEPER_PROPERTIES_PATH); //创建之前就加载配置文件
		}catch (ConfigurationException e){
			logger.error("load configuration failed : " + e.getMessage(),e);
			System.exit(1);
		}
	}


	public AbstractZKClient() {

		// retry strategy
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(
				conf.getInt(Constants.ZOOKEEPER_RETRY_SLEEP),
				conf.getInt(Constants.ZOOKEEPER_RETRY_MAXTIME));

		try{
			// crate zookeeper client
			zkClient = CuratorFrameworkFactory.builder()
						.connectString(getZookeeperQuorum())
						.retryPolicy(retryPolicy)
						.sessionTimeoutMs(1000 * conf.getInt(Constants.ZOOKEEPER_SESSION_TIMEOUT))
						.connectionTimeoutMs(1000 * conf.getInt(Constants.ZOOKEEPER_CONNECTION_TIMEOUT))
						.build();

			zkClient.start();   //抽象client创建的时候，立即和zk建立连接
			initStateLister();  //初始化监听，监听里没做啥事，只是监听和zk连接的状态，打了个日志

		}catch(Exception e){
			logger.error("create zookeeper connect failed : " + e.getMessage(),e);
			System.exit(-1);
		}
    }

	/**
	 * 初始化监听，监听里没做啥事，只是监听和zk连接的状态，打了个日志
	 *  register status monitoring events for zookeeper clients
	 */
	public void initStateLister(){
		if(zkClient == null) {
			return;
		}
		// add ConnectionStateListener monitoring zookeeper  connection state
		ConnectionStateListener csLister = new ConnectionStateListener() {

			@Override
			public void stateChanged(CuratorFramework client, ConnectionState newState) {
				logger.info("state changed , current state : " + newState.name());
				/**
				 * probably session expired
				 */
				if(newState == ConnectionState.LOST){
					// if lost , then exit
					logger.info("current zookeepr connection state : connection lost ");
				}
			}
		};

		zkClient.getConnectionStateListenable().addListener(csLister);
	}


    public void start() {
    	zkClient.start();
		logger.info("zookeeper start ...");
    }

    public void close() {
		zkClient.getZookeeperClient().close();
		zkClient.close();
		logger.info("zookeeper close ...");
    }


	/**
	 *  维持和zk的心跳， 把当前机器的负载信息注册到了对应的zk节点上
	 *  heartbeat for zookeeper
	 * @param znode  zookeeper node    哪个 znode 节点    样例：
	 * @param serverType server type   哪种server， worker 和 master 吗？  是的
	 */
	public void heartBeatForZk(String znode, String serverType){
		try {

			//check dead or not in zookeeper
			if(zkClient.getState() == CuratorFrameworkState.STOPPED || checkIsDeadServer(znode, serverType)){
				stoppable.stop("i was judged to death, release resources and stop myself");
				return;
			}

			byte[] bytes = zkClient.getData().forPath(znode);  //获取节点值
			String resInfoStr = new String(bytes); //反序列化成string
			String[] splits = resInfoStr.split(Constants.COMMA);
			if (splits.length != Constants.HEARTBEAT_FOR_ZOOKEEPER_INFO_LENGTH){ //长度固定死了就是7
				return;
			}
			String str = splits[0] + Constants.COMMA    //第一个是啥？   host ip地址
					+ splits[1] + Constants.COMMA     //第二个是啥？     进程uid
					+ OSUtils.cpuUsage() + Constants.COMMA   //cpu使用率
					+ OSUtils.memoryUsage() + Constants.COMMA  //内存使用率
					+ OSUtils.loadAverage() + Constants.COMMA  //负载率
					+ splits[5] + Constants.COMMA       //第六个是啥？   也是一个时间戳   这个代表创建时间
					+ DateUtils.dateToString(new Date());   //第七个是一个时间戳    这个是修改时间
			zkClient.setData().forPath(znode,str.getBytes());  //也就是说把当前机器的负载信息注册到了zk节点上

		} catch (Exception e) {
			logger.error("heartbeat for zk failed : " + e.getMessage(), e);
			stoppable.stop("heartbeat for zk exception, release resources and stop myself");
		}
	}

	/**
	 *	check dead server or not , if dead, stop self
	 * 检查这个服务是否已经死了
	 * @param zNode   	 node path       节点路径
	 * @param serverType master or worker prefix    //这里可以看出来，就是 master  和 worker
	 * @return  true if not exists
	 * @throws Exception errors
	 */
	protected boolean checkIsDeadServer(String zNode, String serverType) throws Exception{
		//ip_sequenceno
		String[] zNodesPath = zNode.split("\\/");
		String ipSeqNo = zNodesPath[zNodesPath.length - 1];  //结尾后缀是 ip地址 ？

		String type = serverType.equals(MASTER_PREFIX) ? MASTER_PREFIX : WORKER_PREFIX;   //不是master就是worker？

		// 				/dolphinscheduler/dead-servers/master_192.168.xxx.xx    奥，zk存在这个路径就说明这个server已经死了。。。
		String deadServerPath = getDeadZNodeParentPath() + SINGLE_SLASH + type + UNDERLINE + ipSeqNo;

		if(zkClient.checkExists().forPath(zNode) == null ||
				zkClient.checkExists().forPath(deadServerPath) != null ){
			return true;  //路径存在即死亡
		}


		return false;
	}

	/**
	 * 通过host地址移除一个dead server，会先check(拼路径)
	 * @param host
	 * @param serverType
	 * @throws Exception
	 */
	public void removeDeadServerByHost(String host, String serverType) throws Exception {
        List<String> deadServers = zkClient.getChildren().forPath(getDeadZNodeParentPath()); //   /dolphinscheduler/dead-servers  这个路径下的节点
        for(String serverPath : deadServers){    //			master_192.168.xxx.xx
            if(serverPath.startsWith(serverType+UNDERLINE+host)){    //找到
				String server = getDeadZNodeParentPath() + SINGLE_SLASH + serverPath;  //完整路径
				if(zkClient.checkExists().forPath(server) != null){
					zkClient.delete().forPath(server);  //删除
					logger.info("{} server {} deleted from zk dead server path success" , serverType , host);
				}
            }
        }
	}


	/**
	 * 根据server类型，以本机host为ip，创建对应的节点
	 * create zookeeper path according the zk node type.
	 * @param zkNodeType zookeeper node type
	 * @return  register zookeeper path
	 * @throws Exception
	 */
	private String createZNodePath(ZKNodeType zkNodeType) throws Exception {
		// specify the format of stored data in ZK nodes
		String heartbeatZKInfo = ResInfo.getHeartBeatInfo(new Date());  //创建心跳信息
		// create temporary sequence nodes for master znode
		String parentPath = getZNodeParentPath(zkNodeType);  //根据server类型，获取对应的父路径，比如master，是 /dolphinscheduler/masters
		String serverPathPrefix = parentPath + "/" + OSUtils.getHost(); //      /dolphinscheduler/masters/192.168.xxx.xx
		String registerPath = zkClient.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(  // 临时节点
				serverPathPrefix + "_", heartbeatZKInfo.getBytes());    //     节点：  /dolphinscheduler/masters/192.168.xxx.xx_   节点数据：心跳信息(当前负载)
		logger.info("register {} node {} success" , zkNodeType.toString(), registerPath);
		return registerPath;
	}

	/**
	 * 注册服务(zkNodeType指的是master还是worker)，
	 * register server,  if server already exists, return null.
	 * @param zkNodeType zookeeper node type
	 * @return register server path in zookeeper
	 * @throws Exception errors
	 */
	public String registerServer(ZKNodeType zkNodeType) throws Exception {
		String registerPath = null;
		String host = OSUtils.getHost(); //本机ip
		if(checkZKNodeExists(host, zkNodeType)){  //检查是否已经注册过了
			logger.error("register failure , {} server already started on host : {}" ,
					zkNodeType.toString(), host);
			return registerPath;
		}
		registerPath = createZNodePath(zkNodeType);    //   /dolphinscheduler/masters/192.168.xxx.xx_   挂的节点数据：心跳信息(当前负载)  都创建好了

        // handle dead server
		//处理死亡的server，这里是deadle操作，所以就是会check这个znode是否在zk的dead路径存在，如果存在，会删除
		handleDeadServer(registerPath, zkNodeType, Constants.DELETE_ZK_OP);

		return registerPath;
	}

	/**
	 * 处理dead的server，如果op是delete，则代表这个ip地址是重启的，需要在zk的dead路径移除它
	 * 					如果op是add，则代表这个ip地址刚死亡，需要在zk的dead路径创建它
	 *
	 * opType(add): if find dead server , then add to zk deadServerPath
	 * opType(delete): delete path from zk
	 *
	 * @param zNode   		  node path
	 * @param zkNodeType	  master or worker
	 * @param opType		  delete or add
	 * @throws Exception errors
	 */
	public void handleDeadServer(String zNode, ZKNodeType zkNodeType, String opType) throws Exception {
		//ip_sequenceno
		String[] zNodesPath = zNode.split("\\/");
		String ipSeqNo = zNodesPath[zNodesPath.length - 1];  //ip地址

		String type = (zkNodeType == ZKNodeType.MASTER) ? MASTER_PREFIX : WORKER_PREFIX;


		//check server restart, if restart , dead server path in zk should be delete
		if(opType.equals(DELETE_ZK_OP)){  //op是delete，即代表是重启操作， 所以需要在zk dead路径移除当前ip
			String[] ipAndSeqNo = ipSeqNo.split(UNDERLINE);
			String ip = ipAndSeqNo[0];
			removeDeadServerByHost(ip, type);  //从zk的dead路径移除

		}else if(opType.equals(ADD_ZK_OP)){  //op是add，说明这个znode是刚dead的，非重启，需要把这个znode移到zk的dead路径
			String deadServerPath = getDeadZNodeParentPath() + SINGLE_SLASH + type + UNDERLINE + ipSeqNo; //zk dead路径
			if(zkClient.checkExists().forPath(deadServerPath) == null){
				//add dead server info to zk dead server path : /dead-servers/

				zkClient.create().forPath(deadServerPath,(type + UNDERLINE + ipSeqNo).getBytes()); //移动zk dead路径

				logger.info("{} server dead , and {} added to zk dead server path success" ,
						zkNodeType.toString(), zNode);
			}
		}

	}



	/**
	 * for stop server
	 * @param serverStoppable server stoppable interface
	 */
	public void setStoppable(IStoppable serverStoppable){
		this.stoppable = serverStoppable;
	}

	/**
	 * master 活跃节点数
	 * get active master num
	 * @return active master number
	 */
	public int getActiveMasterNum(){
		List<String> childrenList = new ArrayList<>();
		try {
			// read master node parent path from conf
			if(zkClient.checkExists().forPath(getZNodeParentPath(ZKNodeType.MASTER)) != null){
				childrenList = zkClient.getChildren().forPath(getZNodeParentPath(ZKNodeType.MASTER));
			}
		} catch (Exception e) {
			if(e.getMessage().contains("java.lang.IllegalStateException: instance must be started")){
				logger.error("zookeeper service not started",e);
			}else{
				logger.error(e.getMessage(),e);
			}

		}finally {
			return childrenList.size();
		}
	}

	/**
	 * zk 部署路径，从配置文件中直接取的
	 * @return zookeeper quorum
	 */
	public static String getZookeeperQuorum(){
		StringBuilder sb = new StringBuilder();
		String[] zookeeperParamslist = conf.getStringArray(Constants.ZOOKEEPER_QUORUM);
		for (String param : zookeeperParamslist) {
			sb.append(param).append(Constants.COMMA);
		}

		if(sb.length() > 0){
			sb.deleteCharAt(sb.length() - 1);
		}

		return sb.toString();
	}

	/**
	 * 根据节点类型(master/worker)，找对应的节点信息，构建成最顶层的抽象Server
	 * get server list.
	 * @param zkNodeType zookeeper node type
	 * @return server list
	 */
	public List<Server> getServersList(ZKNodeType zkNodeType){
		Map<String, String> masterMap = getServerMaps(zkNodeType);  //<ip地址,心跳信息 >   @TODO masterMap 有歧义  -> serverMap
		String parentPath = getZNodeParentPath(zkNodeType);   //父路径

		List<Server> masterServers = new ArrayList<>();
		int i = 0;
		for (Map.Entry<String, String> entry : masterMap.entrySet()) {
			Server masterServer = ResInfo.parseHeartbeatForZKInfo(entry.getValue()); //构建 Server(最顶层的抽象)
			masterServer.setZkDirectory( parentPath + "/"+ entry.getKey());
			masterServer.setId(i); //id 直接取的index ？
			i ++;
			masterServers.add(masterServer);
		}
		return masterServers;
	}

	/**
	 * 返回一个map(根据server type), key是host地址，value是zk上具体节点路径下的信息(负载信息)
	 * get master server list map.
	 * @param zkNodeType zookeeper node type      MASTER, WORKER, DEAD_SERVER
	 * @return result : {host : resource info}
	 */
	public Map<String, String> getServerMaps(ZKNodeType zkNodeType){

		Map<String, String> masterMap = new HashMap<>();
		try {
			String path =  getZNodeParentPath(zkNodeType);  //  /dolphinscheduler/masters
			List<String> serverList  = getZkClient().getChildren().forPath(path);   //    192.168.xxx.xx_
			for(String server : serverList){
				byte[] bytes  = getZkClient().getData().forPath(path + "/" + server);  //  /dolphinscheduler/masters/192.168.xxx.xx_ 下的数据
				masterMap.putIfAbsent(server, new String(bytes));
			}
		} catch (Exception e) {
			logger.error("get server list failed : " + e.getMessage(), e);
		}

		return masterMap;
	}

	/**
	 * 检查 znode 是否已存在
	 * check the zookeeper node already exists
	 * @param host host
	 * @param zkNodeType zookeeper node type
	 * @return true if exists
	 */
	public boolean checkZKNodeExists(String host, ZKNodeType zkNodeType) {
		String path = getZNodeParentPath(zkNodeType);  //先获取prefix 路径
		if(StringUtils.isEmpty(path)){
			logger.error("check zk node exists error, host:{}, zk node type:{}",
					host, zkNodeType.toString());
			return false;
		}
		Map<String, String> serverMaps = getServerMaps(zkNodeType); //<host ip地址,具体的心跳(负载)信息>
		for(String hostKey : serverMaps.keySet()){
			if(hostKey.startsWith(host)){  // 192.168.xxx.xx_   所以要用startWith
 				return true;
			}
		}
		return false;
	}

	/**
	 *  get zkclient
	 * @return zookeeper client
	 */
	public  CuratorFramework getZkClient() {
		return zkClient;
	}

	/**
	 *
	 * @return get worker node parent path
	 */
	protected String getWorkerZNodeParentPath(){
		return conf.getString(Constants.ZOOKEEPER_DOLPHINSCHEDULER_WORKERS);
	}

	/**
	 *
	 * @return get master node parent path
	 */
	protected String getMasterZNodeParentPath(){
		return conf.getString(Constants.ZOOKEEPER_DOLPHINSCHEDULER_MASTERS);
	}

	/**
	 *
	 * @return get master lock path
	 */
	public String getMasterLockPath(){
		return conf.getString(Constants.ZOOKEEPER_DOLPHINSCHEDULER_LOCK_MASTERS);
	}

	/**
	 * 根据节点类型获取zk父路径
	 * @param zkNodeType zookeeper node type
	 * @return get zookeeper node parent path
	 */
	public String getZNodeParentPath(ZKNodeType zkNodeType) {
		String path = "";
		switch (zkNodeType){
			case MASTER:
				return getMasterZNodeParentPath();  //  /dolphinscheduler/masters
			case WORKER:
				return getWorkerZNodeParentPath();  //   /dolphinscheduler/workers
			case DEAD_SERVER:
				return getDeadZNodeParentPath();    //     /dolphinscheduler/dead-servers
			default:
				break;
		}
		return path;
	}

	/**
	 *
	 * @return get dead server node parent path
	 */
	protected String getDeadZNodeParentPath(){
		return conf.getString(ZOOKEEPER_DOLPHINSCHEDULER_DEAD_SERVERS);
	}

	/**
	 *
	 * @return get master start up lock path
	 */
	public String getMasterStartUpLockPath(){
		return conf.getString(Constants.ZOOKEEPER_DOLPHINSCHEDULER_LOCK_FAILOVER_STARTUP_MASTERS);
	}

	/**
	 *
	 * @return get master failover lock path
	 */
	public String getMasterFailoverLockPath(){
		return conf.getString(Constants.ZOOKEEPER_DOLPHINSCHEDULER_LOCK_FAILOVER_MASTERS);
	}

	/**
	 *
	 * @return get worker failover lock path
	 */
	public String getWorkerFailoverLockPath(){
		return conf.getString(Constants.ZOOKEEPER_DOLPHINSCHEDULER_LOCK_FAILOVER_WORKERS);
	}

	/**
	 * release mutex
	 * @param mutex mutex
	 */
	public static void releaseMutex(InterProcessMutex mutex) {
		if (mutex != null){
			try {
				mutex.release();
			} catch (Exception e) {
				if(e.getMessage().equals("instance must be started before calling this method")){
					logger.warn("lock release");
				}else{
					logger.error("lock release failed : " + e.getMessage(),e);
				}

			}
		}
	}

	/**
	 *  init system znode
	 *  创建master，worker，dead 的父路径
	 */
	protected void initSystemZNode(){
		try {
			createNodePath(getMasterZNodeParentPath());
			createNodePath(getWorkerZNodeParentPath());
			createNodePath(getDeadZNodeParentPath());

		} catch (Exception e) {
			logger.error("init system znode failed : " + e.getMessage(),e);
		}
	}

	/**
	 * create zookeeper node path if not exists
	 * @param zNodeParentPath zookeeper parent path
	 * @throws Exception errors
	 */
	private void createNodePath(String zNodeParentPath) throws Exception {
	    if(null == zkClient.checkExists().forPath(zNodeParentPath)){
	        zkClient.create().creatingParentContainersIfNeeded()
					.withMode(CreateMode.PERSISTENT).forPath(zNodeParentPath);
		}
	}

	/**
	 * server self dead, stop all threads
	 * @param serverHost server host
	 * @param zkNodeType zookeeper node type
	 * @return true if server dead and stop all threads
	 * 检查是否是自己死了？
	 */
	protected boolean checkServerSelfDead(String serverHost, ZKNodeType zkNodeType) {
		if (serverHost.equals(OSUtils.getHost())) {
			logger.error("{} server({}) of myself dead , stopping...",
					zkNodeType.toString(), serverHost);
			stoppable.stop(String.format(" {} server {} of myself dead , stopping...",
					zkNodeType.toString(), serverHost));  //是的话，停掉自己
			return true;
		}
		return false;
	}

	/**
	 *  get host ip, string format: masterParentPath/ip_000001/value
	 * @param path path
	 * @return host ip, string format: masterParentPath/ip_000001/value
	 * 根据zk路径拿到ip地址
	 */
	protected String getHostByEventDataPath(String path) {
		int  startIndex = path.lastIndexOf("/")+1;  //倒数第二个 /
		int endIndex = 	path.lastIndexOf("_");

		if(startIndex >= endIndex){
			logger.error("parse ip error");
			return "";
		}
		return path.substring(startIndex, endIndex);
	}
	/**
	 * acquire zk lock
	 * @param zkClient zk client
	 * @param zNodeLockPath zk lock path
	 * @return zk lock
	 * @throws Exception errors
	 */
	public InterProcessMutex acquireZkLock(CuratorFramework zkClient,String zNodeLockPath)throws Exception{
		InterProcessMutex mutex = new InterProcessMutex(zkClient, zNodeLockPath);
		mutex.acquire();
		return mutex;
	}

	@Override
	public String toString() {
		return "AbstractZKClient{" +
				"zkClient=" + zkClient +
				", deadServerZNodeParentPath='" + getZNodeParentPath(ZKNodeType.DEAD_SERVER) + '\'' +
				", masterZNodeParentPath='" + getZNodeParentPath(ZKNodeType.MASTER) + '\'' +
				", workerZNodeParentPath='" + getZNodeParentPath(ZKNodeType.WORKER) + '\'' +
				", stoppable=" + stoppable +
				'}';
	}
}
