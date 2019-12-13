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
package org.apache.dolphinscheduler.server.worker.runner;

import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.queue.ITaskQueue;
import org.apache.dolphinscheduler.common.thread.Stopper;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.common.utils.CollectionUtils;
import org.apache.dolphinscheduler.common.utils.FileUtils;
import org.apache.dolphinscheduler.common.utils.OSUtils;
import org.apache.dolphinscheduler.common.zk.AbstractZKClient;
import org.apache.dolphinscheduler.dao.ProcessDao;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.entity.Tenant;
import org.apache.dolphinscheduler.dao.entity.WorkerGroup;
import org.apache.dolphinscheduler.server.zk.ZKWorkerClient;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 *  领取任务的线程
 *  fetch task thread
 */
public class FetchTaskThread implements Runnable{

    private static final Logger logger = LoggerFactory.getLogger(FetchTaskThread.class);

    /**
     *  并发执行Task的个数, 执行时每次会从队列中拉 taskNum 这么多个Task
     *  set worker concurrent tasks
     */
    private final int taskNum;

    /**
     *  zkWorkerClient
     */
    private final ZKWorkerClient zkWorkerClient;

    /**
     * task queue impl
     */
    protected ITaskQueue taskQueue;

    /**
     *  process database access
     */
    private final ProcessDao processDao;

    /**
     *  worker thread pool executor  线程池线程的个数 = workerExecNums
     */
    private final ExecutorService workerExecService;

    /**
     *  一个worker上执行任务要启多少个线程，
     *  worker exec nums
     */
    private int workerExecNums;

    /**
     * conf
     */
    private Configuration conf;

    /**
     *  task instance
     */
    private TaskInstance taskInstance;

    /**
     * task instance id
     */
    Integer taskInstId;

    public FetchTaskThread(int taskNum, ZKWorkerClient zkWorkerClient,
                           ProcessDao processDao, Configuration conf,
                           ITaskQueue taskQueue){
        this.taskNum = taskNum;
        this.zkWorkerClient = zkWorkerClient;
        this.processDao = processDao;
        this.workerExecNums = conf.getInt(Constants.WORKER_EXEC_THREADS,
                Constants.defaultWorkerExecThreadNum);
        // worker thread pool executor
        this.workerExecService = ThreadUtils.newDaemonFixedThreadExecutor("Worker-Fetch-Task-Thread",workerExecNums);
        this.conf = conf;
        this.taskQueue = taskQueue;
        this.taskInstance = null;
    }

    /**
     * 检查一个 taskInstance 是否能跑在当前worker上； 直接从db中查，
     * Check if the task runs on this worker
     * @param taskInstance
     * @param host
     * @return
     */
    private boolean checkWorkerGroup(TaskInstance taskInstance, String host){

        int taskWorkerGroupId = processDao.getTaskWorkerGroupId(taskInstance);

        if(taskWorkerGroupId <= 0){
            return true;
        }
        WorkerGroup workerGroup = processDao.queryWorkerGroupById(taskWorkerGroupId);
        if(workerGroup == null ){
            logger.info("task {} cannot find the worker group, use all worker instead.", taskInstance.getId());
            return true;
        }
        String ips = workerGroup.getIpList();
        if(StringUtils.isBlank(ips)){
            logger.error("task:{} worker group:{} parameters(ip_list) is null, this task would be running on all workers",
                    taskInstance.getId(), workerGroup.getId());
        }
        String[] ipArray = ips.split(Constants.COMMA);
        List<String> ipList =  Arrays.asList(ipArray);
        return ipList.contains(host);
    }

    /**
     * 任务执行逻辑；
     * 主要就是轮询的从zk队列按照优先级中拉任务，然后用 workerExecService 线程池，用 TaskScheduleThread 执行任务；
     */
    @Override
    public void run() {
        while (Stopper.isRunning()){
            InterProcessMutex mutex = null;  //为什么需要分布式锁？ 防止两个worker拿到同一个任务
            try {
                ThreadPoolExecutor poolExecutor = (ThreadPoolExecutor) workerExecService;
                //check memory and cpu usage and threads  检查资源、线程数是否足够
                boolean runCheckFlag = OSUtils.checkResource(this.conf, false) && checkThreadCount(poolExecutor);

                Thread.sleep(Constants.SLEEP_TIME_MILLIS);

                if(!runCheckFlag) { //没资源 / 线程不够，就不执行；
                    continue;
                }

                //whether have tasks, if no tasks , no need lock  //获取 zk 上 tasks_queue 下的所有task
                List<String> tasksQueueList = taskQueue.getAllTasks(Constants.DOLPHINSCHEDULER_TASKS_QUEUE);
                if (CollectionUtils.isEmpty(tasksQueueList)){
                    continue;  //没任务，跳过
                }
                // creating distributed locks, lock path /dolphinscheduler/lock/worker
                //拿到分布式锁
                mutex = zkWorkerClient.acquireZkLock(zkWorkerClient.getZkClient(),
                        zkWorkerClient.getWorkerLockPath());


                // task instance id str   每次执行，从队列中拉取 taskNum 个Task， 这里拉取的时候已经按照优先级取了；
                List<String> taskQueueStrArr = taskQueue.poll(Constants.DOLPHINSCHEDULER_TASKS_QUEUE, taskNum);

                for(String taskQueueStr : taskQueueStrArr){
                    if (StringUtils.isEmpty(taskQueueStr)) {
                        continue;
                    }

                    if (!checkThreadCount(poolExecutor)) { //线程数足够
                        break;
                    }

                    // get task instance id
                    taskInstId = getTaskInstanceId(taskQueueStr);   //task id

                    // mainly to wait for the master insert task to succeed
                    waitForTaskInstance();

                    taskInstance = processDao.getTaskInstanceDetailByTaskId(taskInstId);  //从db中找到任务实例

                    // verify task instance is null   //任务实例为空，
                    if (verifyTaskInstanceIsNull(taskInstance)) {
                        logger.warn("remove task queue : {} due to taskInstance is null", taskQueueStr);
                        removeNodeFromTaskQueue(taskQueueStr); //从队列中删除
                        continue;
                    }

                    //对应哪个租户
                    Tenant tenant = processDao.getTenantForProcess(taskInstance.getProcessInstance().getTenantId(),
                            taskInstance.getProcessDefine().getUserId());

                    // verify tenant is null  租户为空，也从zk队列中移除
                    if (verifyTenantIsNull(tenant)) {
                        logger.warn("remove task queue : {} due to tenant is null", taskQueueStr);
                        removeNodeFromTaskQueue(taskQueueStr);
                        continue;
                    }

                    // set queue for process instance, user-specified queue takes precedence over tenant queue
                    String userQueue = processDao.queryUserQueueByProcessInstanceId(taskInstance.getProcessInstanceId());
                    taskInstance.getProcessInstance().setQueue(StringUtils.isEmpty(userQueue) ? tenant.getQueue() : userQueue);
                    taskInstance.getProcessInstance().setTenantCode(tenant.getTenantCode());

                    logger.info("worker fetch taskId : {} from queue ", taskInstId);


                    if(!checkWorkerGroup(taskInstance, OSUtils.getHost())){ //检查这个任务是否能在当前worker跑 (通过设置的worker组)
                        continue;
                    }

                    // local execute path     本地执行路径   /tmp/dolphinscheduler/exec/项目id/流程定义id/流程实例id/任务实例id/
                    String execLocalPath = getExecLocalPath();

                    logger.info("task instance  local execute path : {} ", execLocalPath);

                    // init task
                    taskInstance.init(OSUtils.getHost(), //在这台worker上跑；
                            new Date(),  //开始时间
                            execLocalPath);  //执行路径

                    // check and create Linux users     在worker机上设置执行路径和执行用户
                    FileUtils.createWorkDirAndUserIfAbsent(execLocalPath,
                            tenant.getTenantCode(), logger);

                    logger.info("task : {} ready to submit to task scheduler thread",taskInstId);
                    // submit task      真正的去执行用户任务；
                    workerExecService.submit(new TaskScheduleThread(taskInstance, processDao));

                    // remove node from zk     提交完之后，从zk队列中移除；
                    removeNodeFromTaskQueue(taskQueueStr);
                }

            }catch (Exception e){
                logger.error("fetch task thread failure" ,e);
            }finally {
                AbstractZKClient.releaseMutex(mutex);
            }
        }
    }

    /**
     * remove node from task queue
     *
     * @param taskQueueStr task queue
     */
    private void removeNodeFromTaskQueue(String taskQueueStr){
        taskQueue.removeNode(Constants.DOLPHINSCHEDULER_TASKS_QUEUE, taskQueueStr);
    }

    /**
     * verify task instance is null
     * @param taskInstance
     * @return true if task instance is null
     */
    private boolean verifyTaskInstanceIsNull(TaskInstance taskInstance) {
        if (taskInstance == null ) {
            logger.error("task instance is null. task id : {} ", taskInstId);
            return true;
        }
        return false;
    }

    /**
     * verify tenant is null
     *
     * @param tenant tenant
     * @return true if tenant is null
     */
    private boolean verifyTenantIsNull(Tenant tenant) {
        if(tenant == null){
            logger.error("tenant not exists,process define id : {},process instance id : {},task instance id : {}",
                    taskInstance.getProcessDefine().getId(),
                    taskInstance.getProcessInstance().getId(),
                    taskInstance.getId());
            return true;
        }
        return false;
    }

    /**
     * 获取worker的本地执行路径
     *          /tmp/dolphinscheduler/exec/项目id/流程定义id/流程实例id/任务实例id/
     * get execute local path
     *
     * @return execute local path
     */
    private String getExecLocalPath(){
        return FileUtils.getProcessExecDir(taskInstance.getProcessDefine().getProjectId(),
                taskInstance.getProcessDefine().getId(),
                taskInstance.getProcessInstance().getId(),
                taskInstance.getId());
    }

    /**
     * check thread count
     *
     * @param poolExecutor pool executor
     * @return true if active count < worker exec nums
     */
    private boolean checkThreadCount(ThreadPoolExecutor poolExecutor) {
        int activeCount = poolExecutor.getActiveCount();
        if (activeCount >= workerExecNums) {
            logger.info("thread insufficient , activeCount : {} , " +
                            "workerExecNums : {}, will sleep : {} millis for thread resource",
                    activeCount,
                    workerExecNums,
                    Constants.SLEEP_TIME_MILLIS);
            return false;
        }
        return true;
    }

    /**
     * wait for task instance exists, because of db action would be delayed.
     *
     * @throws Exception exception
     */
    private void waitForTaskInstance()throws Exception{
        int retryTimes = 30;
        while (taskInstance == null && retryTimes > 0) {
            Thread.sleep(Constants.SLEEP_TIME_MILLIS);
            taskInstance = processDao.findTaskInstanceById(taskInstId);
            retryTimes--;
        }
    }

    /**
     * get task instance id
     *
     * @param taskQueueStr task queue
     * @return task instance id
     */
    private int getTaskInstanceId(String taskQueueStr){
        return Integer.parseInt(taskQueueStr.split(Constants.UNDERLINE)[3]);
    }
}