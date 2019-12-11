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
package org.apache.dolphinscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import org.apache.dolphinscheduler.common.enums.ExecutionStatus;
import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.common.enums.Priority;
import org.apache.dolphinscheduler.common.enums.TaskType;
import org.apache.dolphinscheduler.common.model.TaskNode;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.util.Date;

/**
 * task instance
 * DAG中的每个节点，每在一个流程实例中运行一次，产生一个任务实例， 对
 */
@Data
@TableName("t_ds_task_instance")
public class TaskInstance {

    /**
     * id
     */
    @TableId(value="id", type=IdType.AUTO)
    private int id;

    /**
     * task name
     */
    private String name;

    /**
     * task type
     */
    private String taskType;  //任务类型     shell?   hive ?   spark?   这些吗    是的

    /**
     * process definition id
     */
    private int processDefinitionId;   //哪个流程定义

    /**
     * process instance id
     */
    private int processInstanceId;  //哪个流程实例

    /**
     * process instance name
     */
    @TableField(exist = false)
    private String processInstanceName;

    /**
     * task json
     * !!! #### 重要，这里保存着这个Task运行需要的所有信息
     * {"depList":["测试shell1"],"dependence":"{}","forbidden":false,"id":"tasks-70869","maxRetryTimes":3,"name":"测试shell2","params":"{\"rawScript\":\"echo \\\"测试shell2\\\"\",\"localParams\":[],\"resourceList\":[]}","preTasks":"[\"测试shell1\"]","retryInterval":1,"runFlag":"NORMAL","taskInstancePriority":"MEDIUM","taskTimeoutParameter":{"enable":false,"interval":0},"timeout":"{\"enable\":false,\"strategy\":\"\"}","type":"SHELL","workerGroupId":-1}
     */
    private String taskJson;

    /**
     * state
     */
    private ExecutionStatus state;  //执行状态，分为11种，还是挺全的

    /**
     * task submit time
     */
    private Date submitTime;  //提交时间

    /**
     * task start time
     */
    private Date startTime;  //开始时间

    /**
     * task end time
     */
    private Date endTime;

    /**
     * task host
     */
    private String host;   //在哪个机器上运行？ yes，是的

    /**
     * task shell execute path and the resource down from hdfs
     * default path: $base_run_dir/processInstanceId/taskInstanceId/retryTimes
     *
     * 任务的默认执行路径: $base_run_dir/processInstanceId/taskInstanceId/retryTimes
     */
    private String executePath;

    /**
     * task log path
     * default path: $base_run_dir/processInstanceId/taskInstanceId/retryTimes
     *
     * 任务的默认日志路径：$base_run_dir/processInstanceId/taskInstanceId/retryTimes
     * @TODO 怎么做到的呢？ 每个任务写不同的路径？
     */
    private String logPath;

    /**
     * retry times
     */
    private int retryTimes; //重试次数

    /**
     * alert flag
     */
    private Flag alertFlag; //是否打开报警

    /**
     * process instance
     */
    @TableField(exist = false)
    private ProcessInstance processInstance;

    /**
     * process definition
     */
    @TableField(exist = false)
    private ProcessDefinition processDefine;

    /**
     * process id
     */
    private int pid;   //线程id

    /**
     * appLink
     */
    private String appLink;

    /**
     * flag   @TODO 标记啥的？
     */
    private Flag flag;

    /**
     * dependency
     */
    @TableField(exist = false)
    private String dependency;  //依赖？   什么依赖？

    /**
     * duration
     * @return
     */
    @TableField(exist = false)
    private Long duration;

    /**
     * max retry times
     * @return
     */
    private int maxRetryTimes;  //最大重试次数

    /**
     * task retry interval, unit: minute
     * @return
     */
    private int retryInterval; //重试的间隔

    /**
     * task intance priority
     */
    private Priority taskInstancePriority;  //优先级，也是分5级   task的优先级

    /**
     * process intance priority
     */
    @TableField(exist = false)
    private Priority processInstancePriority; // 流程的优先级？？？？

    /**
     * dependent state
     * @return
     */
    @TableField(exist = false)
    private String dependentResult;


    /**
     * worker group id
     * @return
     */
    private int workerGroupId;  //在哪些worker上跑



    public void  init(String host,Date startTime,String executePath){
        this.host = host;
        this.startTime = startTime;
        this.executePath = executePath;
    }


    public ProcessInstance getProcessInstance() {
        return processInstance;
    }

    public void setProcessInstance(ProcessInstance processInstance) {
        this.processInstance = processInstance;
    }

    public ProcessDefinition getProcessDefine() {
        return processDefine;
    }

    public void setProcessDefine(ProcessDefinition processDefine) {
        this.processDefine = processDefine;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public int getProcessDefinitionId() {
        return processDefinitionId;
    }

    public void setProcessDefinitionId(int processDefinitionId) {
        this.processDefinitionId = processDefinitionId;
    }

    public int getProcessInstanceId() {
        return processInstanceId;
    }

    public void setProcessInstanceId(int processInstanceId) {
        this.processInstanceId = processInstanceId;
    }

    public String getTaskJson() {
        return taskJson;
    }

    public void setTaskJson(String taskJson) {
        this.taskJson = taskJson;
    }

    public ExecutionStatus getState() {
        return state;
    }

    public void setState(ExecutionStatus state) {
        this.state = state;
    }

    public Date getSubmitTime() {
        return submitTime;
    }

    public void setSubmitTime(Date submitTime) {
        this.submitTime = submitTime;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getExecutePath() {
        return executePath;
    }

    public void setExecutePath(String executePath) {
        this.executePath = executePath;
    }

    public String getLogPath() {
        return logPath;
    }

    public void setLogPath(String logPath) {
        this.logPath = logPath;
    }

    public Flag getAlertFlag() {
        return alertFlag;
    }

    public void setAlertFlag(Flag alertFlag) {
        this.alertFlag = alertFlag;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public Boolean isTaskSuccess(){
        return this.state == ExecutionStatus.SUCCESS;
    }

    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }

    public String getAppLink() {
        return appLink;
    }

    public void setAppLink(String appLink) {
        this.appLink = appLink;
    }


    public Boolean isSubProcess(){
        return TaskType.SUB_PROCESS.toString().equals(this.taskType.toUpperCase());
    }

    public String getDependency(){

        if(this.dependency != null){
            return this.dependency;
        }
        TaskNode taskNode = JSONUtils.parseObject(taskJson, TaskNode.class);

        return taskNode.getDependence();
    }

    public Flag getFlag() {
        return flag;
    }

    public void setFlag(Flag flag) {
        this.flag = flag;
    }
    public String getProcessInstanceName() {
        return processInstanceName;
    }

    public void setProcessInstanceName(String processInstanceName) {
        this.processInstanceName = processInstanceName;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public void setMaxRetryTimes(int maxRetryTimes) {
        this.maxRetryTimes = maxRetryTimes;
    }

    public int getRetryInterval() {
        return retryInterval;
    }

    public void setRetryInterval(int retryInterval) {
        this.retryInterval = retryInterval;
    }

    public Boolean isTaskComplete() {

        return this.getState().typeIsPause()
                || this.getState().typeIsSuccess()
                || this.getState().typeIsCancel()
                || (this.getState().typeIsFailure() && !taskCanRetry());
    }
    /**
     * determine if you can try again
     * @return can try result
     */
    public boolean taskCanRetry() {
        if(this.isSubProcess()){
            return false;
        }
        if(this.getState() == ExecutionStatus.NEED_FAULT_TOLERANCE){
            return true;
        }else {
            return (this.getState().typeIsFailure()
                && this.getRetryTimes() < this.getMaxRetryTimes());
        }
    }

    public void setDependency(String dependency) {
        this.dependency = dependency;
    }

    public Priority getTaskInstancePriority() {
        return taskInstancePriority;
    }

    public void setTaskInstancePriority(Priority taskInstancePriority) {
        this.taskInstancePriority = taskInstancePriority;
    }

    public Priority getProcessInstancePriority() {
        return processInstancePriority;
    }

    public void setProcessInstancePriority(Priority processInstancePriority) {
        this.processInstancePriority = processInstancePriority;
    }

    public int getWorkerGroupId() {
        return workerGroupId;
    }

    public void setWorkerGroupId(int workerGroupId) {
        this.workerGroupId = workerGroupId;
    }

    @Override
    public String toString() {
        return "TaskInstance{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", taskType='" + taskType + '\'' +
                ", processDefinitionId=" + processDefinitionId +
                ", processInstanceId=" + processInstanceId +
                ", processInstanceName='" + processInstanceName + '\'' +
                ", taskJson='" + taskJson + '\'' +
                ", state=" + state +
                ", submitTime=" + submitTime +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", host='" + host + '\'' +
                ", executePath='" + executePath + '\'' +
                ", logPath='" + logPath + '\'' +
                ", retryTimes=" + retryTimes +
                ", alertFlag=" + alertFlag +
                ", flag=" + flag +
                ", processInstance=" + processInstance +
                ", processDefine=" + processDefine +
                ", pid=" + pid +
                ", appLink='" + appLink + '\'' +
                ", flag=" + flag +
                ", dependency=" + dependency +
                ", duration=" + duration +
                ", maxRetryTimes=" + maxRetryTimes +
                ", retryInterval=" + retryInterval +
                ", taskInstancePriority=" + taskInstancePriority +
                ", processInstancePriority=" + processInstancePriority +
                ", workGroupId=" + workerGroupId +
                '}';
    }

    public String getDependentResult() {
        return dependentResult;
    }

    public void setDependentResult(String dependentResult) {
        this.dependentResult = dependentResult;
    }
}
