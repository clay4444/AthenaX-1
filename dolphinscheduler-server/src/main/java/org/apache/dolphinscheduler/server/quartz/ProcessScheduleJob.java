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
package org.apache.dolphinscheduler.server.quartz;


import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.CommandType;
import org.apache.dolphinscheduler.common.enums.ReleaseState;
import org.apache.dolphinscheduler.dao.ProcessDao;
import org.apache.dolphinscheduler.dao.entity.Command;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.entity.Schedule;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.Date;

/**
 * process schedule job
 * 继承Quartz的Job
 * 主要就是代表一个任务到底要如何执行；其实就是构建了一条Command，然后让master去扫
 */
public class ProcessScheduleJob implements Job {

    /**
     * logger of ProcessScheduleJob
     */
    private static final Logger logger = LoggerFactory.getLogger(ProcessScheduleJob.class);

    /**
     * process dao   操作数据库
     */
    private static ProcessDao processDao;


    /**
     * init
     * @param processDao process dao
     */
    public static void init(ProcessDao processDao) {
        ProcessScheduleJob.processDao = processDao;
    }

    /**
     * 代表一个任务到底要如何执行；其实就是构建了一条Command，然后让master去扫
     * quartz调度的trigger触发的时候，会根据相关联的job调用到这里；
     * Called by the Scheduler when a Trigger fires that is associated with the Job
     *
     * @param context JobExecutionContext
     * @throws JobExecutionException if there is an exception while executing the job.
     */
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {

        Assert.notNull(processDao, "please call init() method first");

        JobDataMap dataMap = context.getJobDetail().getJobDataMap(); //JobExecution上下文中有这个job关联的所有信息

        int projectId = dataMap.getInt(Constants.PROJECT_ID);  //所属项目
        int scheduleId = dataMap.getInt(Constants.SCHEDULE_ID); //对应的调度规则


        Date scheduledFireTime = context.getScheduledFireTime();  //调度应该触发的时间，准确的

        Date fireTime = context.getFireTime(); //调度真实触发的时间，如果scheduler太忙，会有延迟

        logger.info("scheduled fire time :{}, fire time :{}, process id :{}", scheduledFireTime, fireTime, scheduleId);

        // query schedule
        Schedule schedule = processDao.querySchedule(scheduleId); //查到对应的调度规则
        if (schedule == null) {
            logger.warn("process schedule does not exist in db，delete schedule job in quartz, projectId:{}, scheduleId:{}", projectId, scheduleId);
            deleteJob(projectId, scheduleId);
            return;
        }


        ProcessDefinition processDefinition = processDao.findProcessDefineById(schedule.getProcessDefinitionId()); //流程定义
        // release state : online/offline
        ReleaseState releaseState = processDefinition.getReleaseState();
        if (processDefinition == null || releaseState == ReleaseState.OFFLINE) {
            logger.warn("process definition does not exist in db or offline，need not to create command, projectId:{}, processId:{}", projectId, scheduleId);
            return;
        }

        Command command = new Command();  //
        command.setCommandType(CommandType.SCHEDULER); //Command 类型设置为由具体的 Scheduler 调度产生
        command.setExecutorId(schedule.getUserId());  //创建人
        command.setFailureStrategy(schedule.getFailureStrategy());
        command.setProcessDefinitionId(schedule.getProcessDefinitionId()); //这个Command要启动哪个流程定义
        command.setScheduleTime(scheduledFireTime);
        command.setStartTime(fireTime); //真正的调度触发时间
        command.setWarningGroupId(schedule.getWarningGroupId());  //报警组
        command.setWorkerGroupId(schedule.getWorkerGroupId());  //worker组
        command.setWarningType(schedule.getWarningType()); //报警类型
        command.setProcessInstancePriority(schedule.getProcessInstancePriority()); //流程实例优先级

        processDao.createCommand(command); //保存Command，后续让Master扫，
    }


    /**
     * delete job
     */
    private void deleteJob(int projectId, int scheduleId) {
        String jobName = QuartzExecutors.buildJobName(scheduleId);
        String jobGroupName = QuartzExecutors.buildJobGroupName(projectId);
        QuartzExecutors.getInstance().deleteJob(jobName, jobGroupName);
    }
}
