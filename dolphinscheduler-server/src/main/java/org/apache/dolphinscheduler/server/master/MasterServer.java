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
package org.apache.dolphinscheduler.server.master;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.thread.Stopper;
import org.apache.dolphinscheduler.common.thread.ThreadPoolExecutors;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.common.utils.OSUtils;
import org.apache.dolphinscheduler.dao.ProcessDao;
import org.apache.dolphinscheduler.server.master.runner.MasterSchedulerThread;
import org.apache.dolphinscheduler.server.quartz.ProcessScheduleJob;
import org.apache.dolphinscheduler.server.quartz.QuartzExecutors;
import org.apache.dolphinscheduler.server.zk.ZKMasterClient;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.ComponentScan;

import javax.annotation.PostConstruct;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * master server
 * 1. Distributed Quartz分布式调度组件，主要负责定时任务的启停操作，当quartz调起任务后，Master内部会有线程池具体负责处理任务的后续操作
 *
 * 2. MasterSchedulerThread 是一个扫描线程，定时扫描数据库中的 command 表，根据不同的命令类型进行不同的业务操作，   ### 可以理解为是用来提交任务的；
 *
 * 3. MasterExecThread 主要是负责DAG任务切分、任务提交监控、各种不同命令类型的逻辑处理      ### 这是任务的具体执行
 *
 * 4. MasterTaskExecThread 主要负责任务的持久化     #### 这个主要用来持久化
 */
@ComponentScan("org.apache.dolphinscheduler")
public class MasterServer extends AbstractServer {

    /**
     * logger of MasterServer
     */
    private static final Logger logger = LoggerFactory.getLogger(MasterServer.class);

    /**
     *  zk master client
     */
    private ZKMasterClient zkMasterClient = null;  // zk 客户端    封装了一下

    /**
     *  heartbeat thread pool
     */
    private ScheduledExecutorService heartbeatMasterService;  //调度线程池，用来发心跳，只有一个，而且是线程池

    /**
     *  dolphinscheduler database interface
     */
    @Autowired
    protected ProcessDao processDao;   //操作数据库的接口

    /**
     *  master exec thread pool
     */
    private ExecutorService masterSchedulerService;   //master的线程池，  调度执行(扫描)用的，只有一个，而且是守护线程


    /**
     * master server startup
     *
     * master server not use web service
     * @param args arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(MasterServer.class, args);

    }

    /**
     * run master server
     * master启动的入口
     * 1. 初始zk相关，容错等
     * 2. 定时调度心跳
     * 3. MasterSchedulerThread 开始执行
     */
    @PostConstruct
    public void run(){

        try {
            conf = new PropertiesConfiguration(Constants.MASTER_PROPERTIES_PATH);  //从classpath加载配置
        }catch (ConfigurationException e){
            logger.error("load configuration failed : " + e.getMessage(),e);
            System.exit(1);
        }

        masterSchedulerService = ThreadUtils.newDaemonSingleThreadExecutor("Master-Scheduler-Thread"); //只有一个守护线程，这个线程干啥用？看最上面

        /**
         * 1.初始zk上的父路径，master、worker、dead
         * 2.对其他master&所有worker设置监听，宕机时进行容错 (幂等性问题)
         * 3.在zk上注册master
         */
        zkMasterClient = ZKMasterClient.getZKMasterClient(processDao); //

        // heartbeat interval
        heartBeatInterval = conf.getInt(Constants.MASTER_HEARTBEAT_INTERVAL,
                Constants.defaultMasterHeartbeatInterval);

        //master执行任务的线程池个数
        // master exec thread pool num
        int masterExecThreadNum = conf.getInt(Constants.MASTER_EXEC_THREADS,
                Constants.defaultMasterExecThreadNum);


        //心跳线程的调度池
        heartbeatMasterService = ThreadUtils.newDaemonThreadScheduledExecutor("Master-Main-Thread",Constants.defaulMasterHeartbeatThreadNum);

        // heartbeat thread implement
        Runnable heartBeatThread = heartBeatThread(); //心跳操作

        zkMasterClient.setStoppable(this);

        // regular heartbeat
        // delay 5 seconds, send heartbeat every 30 seconds
        //开始发心跳
        heartbeatMasterService.
                scheduleAtFixedRate(heartBeatThread, 5, heartBeatInterval, TimeUnit.SECONDS);

        // master scheduler thread
        //作用：这个可以理解为是用来提交任务的；
        MasterSchedulerThread masterSchedulerThread = new MasterSchedulerThread(
                zkMasterClient,
                processDao,conf,
                masterExecThreadNum);

        // submit master scheduler thread
        masterSchedulerService.execute(masterSchedulerThread);

        // start QuartzExecutors
        // what system should do if exception
        try {
            ProcessScheduleJob.init(processDao);
            QuartzExecutors.getInstance().start();
        } catch (Exception e) {
            try {
                QuartzExecutors.getInstance().shutdown();
            } catch (SchedulerException e1) {
                logger.error("QuartzExecutors shutdown failed : " + e1.getMessage(), e1);
            }
            logger.error("start Quartz failed : " + e.getMessage(), e);
        }


        /**
         *  register hooks, which are called before the process exits
         */
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                if (zkMasterClient.getActiveMasterNum() <= 1) {
                    for (int i = 0; i < Constants.DOLPHINSCHEDULER_WARN_TIMES_FAILOVER; i++) {
                        zkMasterClient.getAlertDao().sendServerStopedAlert(
                                1, OSUtils.getHost(), "Master-Server");
                    }
                }
                stop("shutdownhook");
            }
        }));
    }


    /**
     * gracefully stop
     * @param cause why stopping
     */
    @Override
    public synchronized void stop(String cause) {

        try {
            //execute only once
            if(Stopper.isStoped()){
                return;
            }

            logger.info("master server is stopping ..., cause : {}", cause);

            // set stop signal is true
            Stopper.stop();

            try {
                //thread sleep 3 seconds for thread quitely stop
                Thread.sleep(3000L);
            }catch (Exception e){
                logger.warn("thread sleep exception:" + e.getMessage(), e);
            }
            try {
                heartbeatMasterService.shutdownNow();
            }catch (Exception e){
                logger.warn("heartbeat service stopped exception");
            }

            logger.info("heartbeat service stopped");

            //close quartz
            try{
                QuartzExecutors.getInstance().shutdown();
            }catch (Exception e){
                logger.warn("Quartz service stopped exception:{}",e.getMessage());
            }

            logger.info("Quartz service stopped");

            try {
                ThreadPoolExecutors.getInstance().shutdown();
            }catch (Exception e){
                logger.warn("threadpool service stopped exception:{}",e.getMessage());
            }

            logger.info("threadpool service stopped");

            try {
                masterSchedulerService.shutdownNow();
            }catch (Exception e){
                logger.warn("master scheduler service stopped exception:{}",e.getMessage());
            }

            logger.info("master scheduler service stopped");

            try {
                zkMasterClient.close();
            }catch (Exception e){
                logger.warn("zookeeper service stopped exception:{}",e.getMessage());
            }

            logger.info("zookeeper service stopped");


        } catch (Exception e) {
            logger.error("master server stop exception : " + e.getMessage(), e);
            System.exit(-1);
        }
    }


    /**
     * 心跳线程实现
     *  heartbeat thread implement
     * @return
     */
    private Runnable heartBeatThread(){
        Runnable heartBeatThread  = new Runnable() {
            @Override
            public void run() {
                if(Stopper.isRunning()) {
                    // send heartbeat to zk
                    if (StringUtils.isBlank(zkMasterClient.getMasterZNode())) {  //masterZNode: 当前master在zk上的注册路径
                        logger.error("master send heartbeat to zk failed: can't find zookeeper path of master server");
                        return;
                    }

                    //维护和zk的心跳，其实就是把当前机器的负载信息注册到了当前master节点路径上；
                    zkMasterClient.heartBeatForZk(zkMasterClient.getMasterZNode(), Constants.MASTER_PREFIX);
                }
            }
        };
        return heartBeatThread;
    }
}

