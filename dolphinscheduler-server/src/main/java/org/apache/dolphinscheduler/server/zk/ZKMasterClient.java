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
package org.apache.dolphinscheduler.server.zk;

import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.ExecutionStatus;
import org.apache.dolphinscheduler.common.enums.ZKNodeType;
import org.apache.dolphinscheduler.common.model.Server;
import org.apache.dolphinscheduler.common.zk.AbstractZKClient;
import org.apache.dolphinscheduler.dao.AlertDao;
import org.apache.dolphinscheduler.dao.DaoFactory;
import org.apache.dolphinscheduler.dao.ProcessDao;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.server.utils.ProcessUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadFactory;


/**
 *  zookeeper master client
 *  single instance
 * master一个client，worker一个client
 */
public class ZKMasterClient extends AbstractZKClient {

	/**
	 * logger
	 */
	private static final Logger logger = LoggerFactory.getLogger(ZKMasterClient.class);

	/**
	 * thread factory
	 * 主要是zk缓存会用到；
	 */
	private static final ThreadFactory defaultThreadFactory = ThreadUtils.newGenericThreadFactory("Master-Main-Thread");

	/**
	 *  master znode   节点
	 */
	private String masterZNode = null;

	/**
	 *  alert database access
	 */
	private AlertDao alertDao = null;
	/**
	 *  flow database access
	 *  访问数据库的
	 */
	private ProcessDao processDao;

	/**
	 *  zkMasterClient
	 */
	private static ZKMasterClient zkMasterClient = null;

	/**
	 * master path children cache   zk缓存
	 */
	private PathChildrenCache masterPathChildrenCache;

	/**
	 * worker path children cache
	 */
	private PathChildrenCache workerPathChildrenCache;

	/**
	 * 创建的时候就立即执行init
	 * constructor
	 * @param processDao process dao
	 */
	private ZKMasterClient(ProcessDao processDao){
		this.processDao = processDao;
		init();
	}

	/**
	 * default constructor
	 */
	private ZKMasterClient(){}

	/**
	 * get zkMasterClient
	 *
	 * @param processDao process dao
	 * @return ZKMasterClient zookeeper master client
	 */
	public static synchronized ZKMasterClient getZKMasterClient(ProcessDao processDao){
		if(zkMasterClient == null){
			zkMasterClient = new ZKMasterClient(processDao);
		}
		zkMasterClient.processDao = processDao;

		return zkMasterClient;
	}

	/**
	 * init 初始化方法(创建后立即执行)
	 */
	public void init(){
		// init dao
		this.initDao();  //给alertDao 赋值

		InterProcessMutex mutex = null; //分布式锁
		try {
			// create distributed lock with the root node path of the lock space as /dolphinscheduler/lock/failover/master
			String znodeLock = getMasterStartUpLockPath();  //		/dolphinscheduler/lock/failover/master
			mutex = new InterProcessMutex(zkClient, znodeLock);
			mutex.acquire(); //拿到锁    这里会和其他master竞争, 也就是说同时只有一个master在干下面这些事

			// init system znode
			this.initSystemZNode();  // 把master，worker，dead 的父路径都创建出来；

			//总结：也就是说master不仅需要对其他的master容错，还会对所有的worker容错；
			//注意&问题：假设某个worker宕机，此时两个master都会对宕机的worker做容错，这里虽然容错的时候使用了容错锁，但是第二个获得容错锁的master仍然会执行
			//所以容错的代码需要保证幂等性；

			// monitor master
			this.listenerMaster();  //监控其他master，如果有其他master宕机下线了，这里收到感知，会把宕机的master节点从zk的master路径删除，还会对宕机的master上正在运行的流程实例进行容错

			// monitor worker
			this.listenerWorker(); //监控所有worker，如果有worker宕机下线了，这里收到感知，会把宕机的worker节点从zk的worker父路径删除，还会对宕机的worker上正在运行的任务实例进行容错

			// register master
			this.registerMaster();  //在zk上注册，角色为master

			// check if fault tolerance is required，failure and tolerance
			if (getActiveMasterNum() == 1) { //也就是说之前没有master(可能是全挂掉了)
				failoverWorker(null, true);  //容错所有任务实例
				failoverMaster(null);  //容错所有流程实例
			}

		}catch (Exception e){
			logger.error("master start up  exception : " + e.getMessage(),e);
		}finally {
			releaseMutex(mutex);
		}
	}

	@Override
	public void close(){
		try {
			if(masterPathChildrenCache != null){
				masterPathChildrenCache.close();
			}
			if(workerPathChildrenCache != null){
				workerPathChildrenCache.close();
			}
			super.close();
		} catch (Exception ignore) {
		}
	}




	/**
	 *  init dao
	 */
	public void initDao(){
		this.alertDao = DaoFactory.getDaoInstance(AlertDao.class);  //反射直接创建的
	}
	/**
	 * get alert dao
	 *
	 * @return AlertDao
	 */
	public AlertDao getAlertDao() {
		return alertDao;
	}




	/**
	 *  register master znode
	 */
	public void registerMaster(){
		try {
		    String serverPath = registerServer(ZKNodeType.MASTER); //在zk上注册(自身为master)
		    if(StringUtils.isEmpty(serverPath)){
		    	System.exit(-1);
			}
			masterZNode = serverPath;
		} catch (Exception e) {
			logger.error("register master failure : "  + e.getMessage(),e);
			System.exit(-1);
		}
	}



	/**
	 * 监控其他master，如果有其他master宕机下线了，这里收到感知，会把宕机的master节点从zk的master路径删除，还会对宕机的master上正在运行的流程实例进行容错
	 *  monitor master
	 */
	public void listenerMaster(){
		masterPathChildrenCache = new PathChildrenCache(zkClient,
				getZNodeParentPath(ZKNodeType.MASTER), true ,defaultThreadFactory);  //new 缓存

		try {
			masterPathChildrenCache.start();
			masterPathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
				@Override
				public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
					switch (event.getType()) {
						case CHILD_ADDED:   //新master上线
							logger.info("master node added : {}",event.getData().getPath());
							break;
						case CHILD_REMOVED:  //有一个master下线
							String path = event.getData().getPath();
							String serverHost = getHostByEventDataPath(path);  //拿到下线master的ip地址
							if(checkServerSelfDead(serverHost, ZKNodeType.MASTER)){ //检查是否是当前节点下线， @TODO 会出现这种情况吗？
								return;  //终止
							}
							removeZKNodePath(path, ZKNodeType.MASTER, true); //从zk的master父路径移除宕机的master，还会对宕机的master上正在运行的流程实例进行容错
							break;
						case CHILD_UPDATED:
							break;
						default:
							break;
					}
				}
			});
		}catch (Exception e){
			logger.error("monitor master failed : " + e.getMessage(),e);
		}
}

	/**
	 * remove zookeeper node path
	 * @param path			zookeeper node path
	 * @param zkNodeType	zookeeper node type
	 * @param failover		is failover
	 * 把path对应的znode移除，  还会容错
	 */
	private void removeZKNodePath(String path, ZKNodeType zkNodeType, boolean failover) {
		logger.info("{} node deleted : {}", zkNodeType.toString(), path);
		InterProcessMutex mutex = null; //锁
		try {
			String failoverPath = getFailoverLockPath(zkNodeType); //根据节点类型获取各自对应的容错锁，
			// create a distributed lock
			mutex = new InterProcessMutex(getZkClient(), failoverPath);
			mutex.acquire();  //获取锁

			String serverHost = getHostByEventDataPath(path);  //获取ip地址
			// handle dead server
			handleDeadServer(path, zkNodeType, Constants.ADD_ZK_OP);  //把宕机的节点移到zk的dead路径
			//alert server down.
			alertServerDown(serverHost, zkNodeType); //报警
			//failover server
			if(failover){  //容错
				failoverServerWhenDown(serverHost, zkNodeType); // 可以点进去看
			}
		}catch (Exception e){
			logger.error("{} server failover failed.", zkNodeType.toString());
			logger.error("failover exception : " + e.getMessage(),e);
		}
		finally {
			releaseMutex(mutex); //释放锁
		}
	}

	/**
	 * 当某个server服务宕机的时候，进行容错，怎么容错呢？
	 * 如果是master，会把需要容错的流程实例查出来(所在的master挂掉了)，把原流程实例的host(master地址)置空，然后往command表插入一条流程实例容错的命令，应该是后续会扫到这个命令
	 * 如果是worker，会把需要容错的TaskInstance查出来(所在的worker挂掉了)，把任务的状态设置为需要容错(yarn job 会直接kill)，就完了，应该是后续会扫到这些Task
	 *
	 * failover server when server down
	 * @param serverHost	server host
	 * @param zkNodeType	zookeeper node type
	 * @throws Exception	exception
	 */
	private void failoverServerWhenDown(String serverHost, ZKNodeType zkNodeType) throws Exception {
	    if(StringUtils.isEmpty(serverHost)){
	    	return ;
		}
		switch (zkNodeType){
			case MASTER:
				failoverMaster(serverHost);  //容错master
				break;
			case WORKER:
				failoverWorker(serverHost, true);  //容错worker
			default:
				break;
		}
	}

	/**
	 * get failover lock path
	 * @param zkNodeType zookeeper node type
	 * @return fail over lock path
	 * 意思就是每种节点类型都有各自的容错锁
	 */
	private String getFailoverLockPath(ZKNodeType zkNodeType){

		switch (zkNodeType){
			case MASTER:
				return getMasterFailoverLockPath();  //master节点容错锁路径   /dolphinscheduler/lock/failover/masters
			case WORKER:
				return getWorkerFailoverLockPath(); //worker节点容错锁路径    /dolphinscheduler/lock/failover/workers
			default:
				return "";
		}
	}

	/**
	 * server宕机，发送报警
	 * send alert when server down
	 * @param serverHost	server host
	 * @param zkNodeType	zookeeper node type
	 */
	private void alertServerDown(String serverHost, ZKNodeType zkNodeType) {

	    String serverType = zkNodeType.toString();
		for (int i = 0; i < Constants.DOLPHINSCHEDULER_WARN_TIMES_FAILOVER; i++) { //只报警3次
			alertDao.sendServerStopedAlert(1, serverHost, serverType); //只是把报警记录插入mysql了？
		}
	}

	/**
	 * 监控所有worker，如果有worker宕机下线了，这里收到感知，会把宕机的worker节点从zk的worker父路径删除，还会对宕机的worker上正在运行的任务实例进行容错
	 * monitor worker
	 */
	public void listenerWorker(){
		workerPathChildrenCache = new PathChildrenCache(zkClient,
				getZNodeParentPath(ZKNodeType.WORKER),true ,defaultThreadFactory);
		try {
			workerPathChildrenCache.start();
			workerPathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
				@Override
				public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
					switch (event.getType()) {
						case CHILD_ADDED:  //新增worker
							logger.info("node added : {}" ,event.getData().getPath());
							break;
						case CHILD_REMOVED: //worker宕机下线；
							String path = event.getData().getPath();
							logger.info("node deleted : {}",event.getData().getPath());
							removeZKNodePath(path, ZKNodeType.WORKER, true); //从zk的worker父路径删除，
							break;
						default:
							break;
					}
				}
			});
		}catch (Exception e){
			logger.error("listener worker failed : " + e.getMessage(),e);
		}
	}


	/**
	 * get master znode
	 *
	 * @return master zookeeper node
	 */
	public String getMasterZNode() {
		return masterZNode;
	}

	/**
	 * 检查一个Task是否需要容错(两个条件:1.worker在zk上存在，2:任务的启动时间晚于worker的启动时间)
	 *
	 * task needs failover if task start before worker starts
     *
	 * @param taskInstance task instance
	 * @return true if task instance need fail over
	 */
	private boolean checkTaskInstanceNeedFailover(TaskInstance taskInstance) throws Exception {

		boolean taskNeedFailover = true;

		//now no host will execute this task instance,so no need to failover the task
		if(taskInstance.getHost() == null){ //host为空，说明还未执行，肯定不需要容错
			return false;
		}

		// if the worker node exists in zookeeper, we must check the task starts after the worker
	    if(checkZKNodeExists(taskInstance.getHost(), ZKNodeType.WORKER)){  //先确定所在worker在 zk worker下存在，否则肯定需要容错
	        //if task start after worker starts, there is no need to failover the task.
         	if(checkTaskAfterWorkerStart(taskInstance)){ //检查这个task是否是worker启动之后启动的，如果是，则不需要容错
         	    taskNeedFailover = false;
			}
		}
		return taskNeedFailover;
	}

	/**
	 * 检查这个task是否是worker启动之后启动的
	 * check task start after the worker server starts.
	 *
	 * @param taskInstance task instance
	 * @return true if task instance start time after worker server start date
	 */
	private boolean checkTaskAfterWorkerStart(TaskInstance taskInstance) {
	    if(StringUtils.isEmpty(taskInstance.getHost())){
	    	return false;
		}
	    Date workerServerStartDate = null;
	    List<Server> workerServers = getServersList(ZKNodeType.WORKER);  //从zk上直接取的
	    for(Server workerServer : workerServers){
	    	if(workerServer.getHost().equals(taskInstance.getHost())){
	    	    workerServerStartDate = workerServer.getCreateTime();
	    	    break;
			}
		}

		if(workerServerStartDate != null){
			return taskInstance.getStartTime().after(workerServerStartDate);//是否是在worker启动之后启动的，
		}else{
			return false;
		}
	}

	/**
	 * failover worker tasks
	 *
	 * 1. kill yarn job if there are yarn jobs in tasks.
	 * 2. change task state from running to need failover.
     * 3. failover all tasks when workerHost is null
	 * @param workerHost worker host
	 */

	/**
	 * 对worker宕机进行容错
	 * 1.如果有yarn任务，需要kill yarn job
	 * 2.把任务的状态从运行中改为待容错
	 * 3.如果workerHost为空，则表示所有任务都需要容错
	 *
	 * failover worker tasks
	 *
	 * 1. kill yarn job if there are yarn jobs in tasks.
	 * 2. change task state from running to need failover.
	 * 3. failover all tasks when workerHost is null
	 * @param workerHost			worker host
	 * @param needCheckWorkerAlive	need check worker alive
	 * @throws Exception			exception
	 */
	private void failoverWorker(String workerHost, boolean needCheckWorkerAlive) throws Exception {
		logger.info("start worker[{}] failover ...", workerHost);

		//查到所有需要容错的task
		List<TaskInstance> needFailoverTaskInstanceList = processDao.queryNeedFailoverTaskInstances(workerHost);
		for(TaskInstance taskInstance : needFailoverTaskInstanceList){
			if(needCheckWorkerAlive){
				if(!checkTaskInstanceNeedFailover(taskInstance)){ //检查一个Task是否需要容错(两个条件:1.worker在zk上存在，2:任务的启动时间晚于worker的启动时间)
					continue;
                }
			}

			//需要容错：

			ProcessInstance instance = processDao.findProcessInstanceDetailById(taskInstance.getProcessInstanceId()); //找到所属的流程实例
			if(instance!=null){
				taskInstance.setProcessInstance(instance);
			}
			// only kill yarn job if exists , the local thread has exited
			ProcessUtils.killYarnJob(taskInstance); //判断是yarn job 直接kill，

			taskInstance.setState(ExecutionStatus.NEED_FAULT_TOLERANCE); //任务状态设置为需要容错   (只修改了状态  嗯。。。。。。)
			processDao.saveTaskInstance(taskInstance);
		}
		logger.info("end worker[{}] failover ...", workerHost);
	}

	/**
	 * failover master tasks
	 * 把需要容错的流程实例查出来(所在的master挂掉了)，把原流程实例的host(master地址)置空，然后往command表插入一条流程实例容错的命令，应该是后续会扫到这个命令
	 * @param masterHost master host
	 */
	private void failoverMaster(String masterHost) {
		logger.info("start master failover ...");

		//查询因为master挂掉，而需要容错的流程实例 (host是挂掉的master的ip)，stateArray代表要找running状态的，
		List<ProcessInstance> needFailoverProcessInstanceList = processDao.queryNeedFailoverProcessInstances(masterHost);

		//updateProcessInstance host is null and insert into command
		for(ProcessInstance processInstance : needFailoverProcessInstanceList){
			//处理需要容错的流程实例，把原流程实例的host(master地址)置空，然后往command表插入一条流程实例容错的命令，应该是后续会扫到这个命令
			processDao.processNeedFailoverProcessInstances(processInstance);
		}

		logger.info("master failover end");
	}

}
