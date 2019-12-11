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
package org.apache.dolphinscheduler.server.master.runner;

import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.thread.Stopper;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.common.utils.OSUtils;
import org.apache.dolphinscheduler.common.zk.AbstractZKClient;
import org.apache.dolphinscheduler.dao.ProcessDao;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.server.zk.ZKMasterClient;
import org.apache.commons.configuration.Configuration;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * MasterSchedulerThread是一个扫描线程，定时扫描数据库中的 command 表，根据不同的命令类型进行不同的业务操作
 *
 *  master的调度线程，作用：
 *  master scheduler thread
 */
public class MasterSchedulerThread implements Runnable {

    /**
     * logger of MasterSchedulerThread
     */
    private static final Logger logger = LoggerFactory.getLogger(MasterSchedulerThread.class);

    /**
     * master exec service
     */
    private final ExecutorService masterExecService;

    /**
     * dolphinscheduler database interface
     */
    private final ProcessDao processDao;

    /**
     * zookeeper master client
     */
    private final ZKMasterClient zkMasterClient ;

    /**
     * master exec thread num
     * 最大执行线程数，这个可不是1啊，默认是100
     */
    private int masterExecThreadNum;  //会一直维护，执行一个减一个

    /**
     * Configuration of MasterSchedulerThread
     */
    private final Configuration conf;

    /**
     * constructor of MasterSchedulerThread
     * @param zkClient              zookeeper master client
     * @param processDao            process dao
     * @param conf                  conf
     * @param masterExecThreadNum   master exec thread num
     */
    public MasterSchedulerThread(ZKMasterClient zkClient, ProcessDao processDao, Configuration conf, int masterExecThreadNum){
        this.processDao = processDao;
        this.zkMasterClient = zkClient;
        this.conf = conf;
        this.masterExecThreadNum = masterExecThreadNum;
        //固定大小线程池在这创建的，大小是 masterExecThreadNum，默认是100
        this.masterExecService = ThreadUtils.newDaemonFixedThreadExecutor("Master-Exec-Thread",masterExecThreadNum);
    }

    /**
     * run of MasterSchedulerThread
     */
    @Override
    public void run() {
        while (Stopper.isRunning()){ //死循环

            // process instance
            ProcessInstance processInstance = null;

            InterProcessMutex mutex = null;  //分布式锁， 说明同时只能有一个master执行
            try {

                if(OSUtils.checkResource(conf, true)){ //check资源是否足够，这一点挺好的，如果负载过高，就不再执行新的流程
                    if (zkMasterClient.getZkClient().getState() == CuratorFrameworkState.STARTED) { //确保zk状态正常；

                        // create distributed lock with the root node path of the lock space as /dolphinscheduler/lock/failover/master
                        String znodeLock = zkMasterClient.getMasterLockPath();

                        mutex = new InterProcessMutex(zkMasterClient.getZkClient(), znodeLock); //这里为什么要使用master容错锁呢？
                        mutex.acquire();

                        ThreadPoolExecutor poolExecutor = (ThreadPoolExecutor) masterExecService;
                        int activeCount = poolExecutor.getActiveCount();  //活跃线程数
                        // make sure to scan and delete command  table in one transaction
                        Command command = processDao.findOneCommand(); //扫描数据库中的 command 表，找出来一条跑
                        if (command != null) {
                            logger.info(String.format("find one command: id: %d, type: %s", command.getId(),command.getCommandType().toString()));

                            try{
                                processInstance = processDao.handleCommand(logger, OSUtils.getHost(), this.masterExecThreadNum - activeCount, command);
                                if (processInstance != null) {
                                    logger.info("start master exec thread , split DAG ...");
                                    masterExecService.execute(new MasterExecThread(processInstance,processDao));
                                }
                            }catch (Exception e){
                                logger.error("scan command error ", e);
                                processDao.moveToErrorCommand(command, e.toString());
                            }
                        }
                    }
                }

                // accessing the command table every SLEEP_TIME_MILLIS milliseconds
                Thread.sleep(Constants.SLEEP_TIME_MILLIS);

            }catch (Exception e){
                logger.error("master scheduler thread exception : " + e.getMessage(),e);
            }finally{
                AbstractZKClient.releaseMutex(mutex);
            }
        }
    }


}
