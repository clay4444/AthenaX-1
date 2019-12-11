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

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import lombok.Data;
import org.apache.dolphinscheduler.common.enums.*;

import java.util.Date;

/**
 * process instance   流程实例？ 是的，流程定义每根据调度(或者手动)每跑一次，产生一个流程实例
 */
@Data
@TableName("t_ds_process_instance")
public class ProcessInstance {

    /**
     * id
     */
    @TableId(value="id", type=IdType.AUTO)
    private int id;
    /**
     * process definition id
     */
    private int processDefinitionId;   //流程定义id
    /**
     * process state
     */
    private ExecutionStatus state;   //  状态
    /**
     * recovery flag for failover
     */
    private Flag recovery;   //是否是容错的
    /**
     * start time
     */
    private Date startTime;

    /**
     * end time
     */
    private Date endTime;

    /**
     * run time
     */
    private int runTimes;  //跑的次数

    /**
     * name
     */
    private String name;

    /**
     * host
     */
    private String host;  //在哪台机器上跑？ 不对，是被哪个master调度的？ 这个是master地址；  Task的host是worker地址

    /**
     * process definition structure
     */
    @TableField(exist = false)
    private ProcessDefinition processDefinition;  //流程定义
    /**
     * process command type
     */
    private CommandType commandType; //命令类型， 手动点运行默认值是0，代表新启动的一个流程实例

    /**
     * command parameters
     */
    private String commandParam;  //命令参数

    /**
     * node depend type  节点之前的依赖类型？(但是这个不应该是任务级别的吗，怎么在流程实例里)
     */
    private TaskDependType taskDependType;  //只跑当前任务还是...   默认是2，跑当前任务和后续任务       @TODO 作用是啥？

    /**
     * task max try times
     */
    private int maxTryTimes;

    /**
     * failure strategy when task failed.
     * 失败时的策略  终止 还是  后续任务继续执行
     */
    private FailureStrategy failureStrategy;

    /**
     *  warning type
     */
    private WarningType warningType;

    /**
     * warning group
     */
    private Integer warningGroupId;

    /**
     * schedule time
     */
    private Date scheduleTime;

    /**
     * command start time
     */
    private Date commandStartTime;

    /**
     * user define parameters string
     */
    private String globalParams;

    /**
     * process instance json
     * ### 重要，所有task任务相关的数据存在这里，是一个大json，有一个属性是tasks，保存的是所有task的全部信息
     * worker领到这个流程实例的时候，应该是根据这个去找的
     */
    private String processInstanceJson;

    /**
     * executor id   @TODO  这个指的是啥？？
     */
    private int executorId;

    /**
     * tenant code
     */
    @TableField(exist = false)
    private String tenantCode;

    /**
     * queue
     */
    @TableField(exist = false)
    private String queue;

    /**
     * process is sub process
     */
    private Flag isSubProcess;

    /**
     * task locations for web
     */
    private String locations;

    /**
     * task connects for web
     */
    private String connects;

    /**
     * history command
     */
    private String historyCmd;

    /**
     * depend processes schedule time
     */
    private String dependenceScheduleTimes;

    /**
     * process duration
     * @return
     */
    @TableField(exist = false)
    private Long duration;

    /**
     * process instance priority
     */
    private Priority processInstancePriority;

    /**
     * worker group id
     */
    private int workerGroupId;

    /**
     * process timeout for warning
     */
    private int timeout;

    /**
     * tenant id
     */
    private int tenantId;

    /**
     * worker group name. for api.
     */
    @TableField(exist = false)
    private String workerGroupName;

    /**
     * receivers for api
     */
    @TableField(exist = false)
    private String receivers;

    /**
     * receivers cc for api
     */
    @TableField(exist = false)
    private String receiversCc;

    public ProcessInstance(){

    }

    /**
     * set the process name with process define version and timestamp
     * @param processDefinition processDefinition
     */
    public ProcessInstance(ProcessDefinition processDefinition){
        this.processDefinition = processDefinition;
        this.name = processDefinition.getName() + "-" +
                processDefinition.getVersion() + "-" +
                System.currentTimeMillis();
    }

    public ProcessDefinition getProcessDefinition() {
        return processDefinition;
    }

    public void setProcessDefinition(ProcessDefinition processDefinition) {
        this.processDefinition = processDefinition;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getProcessDefinitionId() {
        return processDefinitionId;
    }

    public void setProcessDefinitionId(int processDefinitionId) {
        this.processDefinitionId = processDefinitionId;
    }

    public ExecutionStatus getState() {
        return state;
    }

    public void setState(ExecutionStatus state) {
        this.state = state;
    }

    public Flag getRecovery() {
        return recovery;
    }

    public void setRecovery(Flag recovery) {
        this.recovery = recovery;
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

    public int getRunTimes() {
        return runTimes;
    }

    public void setRunTimes(int runTimes) {
        this.runTimes = runTimes;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }


    public CommandType getCommandType() {
        return commandType;
    }

    public void setCommandType(CommandType commandType) {
        this.commandType = commandType;
    }

    public String getCommandParam() {
        return commandParam;
    }

    public void setCommandParam(String commandParam) {
        this.commandParam = commandParam;
    }

    public TaskDependType getTaskDependType() {
        return taskDependType;
    }

    public void setTaskDependType(TaskDependType taskDependType) {
        this.taskDependType = taskDependType;
    }


    public int getMaxTryTimes() {
        return maxTryTimes;
    }

    public void setMaxTryTimes(int maxTryTimes) {
        this.maxTryTimes = maxTryTimes;
    }

    public FailureStrategy getFailureStrategy() {
        return failureStrategy;
    }

    public void setFailureStrategy(FailureStrategy failureStrategy) {
        this.failureStrategy = failureStrategy;
    }


    public boolean IsProcessInstanceStop(){
        return this.state.typeIsFinished();
    }

    public WarningType getWarningType() {
        return warningType;
    }

    public void setWarningType(WarningType warningType) {
        this.warningType = warningType;
    }

    public Integer getWarningGroupId() {
        return warningGroupId;
    }

    public void setWarningGroupId(Integer warningGroupId) {
        this.warningGroupId = warningGroupId;
    }

    public Date getScheduleTime() {
        return scheduleTime;
    }

    public void setScheduleTime(Date scheduleTime) {
        this.scheduleTime = scheduleTime;
    }

    public Date getCommandStartTime() {
        return commandStartTime;
    }

    public void setCommandStartTime(Date commandStartTime) {
        this.commandStartTime = commandStartTime;
    }

    public String getGlobalParams() {
        return globalParams;
    }

    public void setGlobalParams(String globalParams) {
        this.globalParams = globalParams;
    }

    public String getProcessInstanceJson() {
        return processInstanceJson;
    }

    public void setProcessInstanceJson(String processInstanceJson) {
        this.processInstanceJson = processInstanceJson;
    }

    public String getTenantCode() {
        return tenantCode;
    }

    public void setTenantCode(String tenantCode) {
        this.tenantCode = tenantCode;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public int getExecutorId() {
        return executorId;
    }

    public void setExecutorId(int executorId) {
        this.executorId = executorId;
    }


    public Flag getIsSubProcess() {
        return isSubProcess;
    }

    public void setIsSubProcess(Flag isSubProcess) {
        this.isSubProcess = isSubProcess;
    }

    public Priority getProcessInstancePriority() {
        return processInstancePriority;
    }

    public void setProcessInstancePriority(Priority processInstancePriority) {
        this.processInstancePriority = processInstancePriority;
    }
    public String getLocations() {
        return locations;
    }

    public void setLocations(String locations) {
        this.locations = locations;
    }

    public String getConnects() {
        return connects;
    }

    public void setConnects(String connects) {
        this.connects = connects;
    }

    public String getHistoryCmd() {
        return historyCmd;
    }

    public void setHistoryCmd(String historyCmd) {
        this.historyCmd = historyCmd;
    }

    /**
     * add command to history
     * @param cmd cmd
     */
    public void addHistoryCmd(CommandType cmd){
        if(StringUtils.isNotEmpty(this.historyCmd)){
            this.historyCmd = String.format("%s,%s", this.historyCmd, cmd.toString());
        }else{
            this.historyCmd = cmd.toString();
        }
    }

    /**
     * check this process is start complement data
     * @return whether complement data
     */
    public Boolean isComplementData(){
        if(!StringUtils.isNotEmpty(this.historyCmd)){
            return false;
        }
        return historyCmd.startsWith(CommandType.COMPLEMENT_DATA.toString());
    }

    /**
     * get current command type,
     * if start with complement data,return complement
     * @return CommandType
     */
    public CommandType getCmdTypeIfComplement(){
        if(isComplementData()){
            return CommandType.COMPLEMENT_DATA;
        }
        return commandType;
    }

    public String getDependenceScheduleTimes() {
        return dependenceScheduleTimes;
    }

    public void setDependenceScheduleTimes(String dependenceScheduleTimes) {
        this.dependenceScheduleTimes = dependenceScheduleTimes;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public int getWorkerGroupId() {
        return workerGroupId;
    }

    public void setWorkerGroupId(int workerGroupId) {
        this.workerGroupId = workerGroupId;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }


    public void setTenantId(int tenantId) {
        this.tenantId = tenantId;
    }

    public int getTenantId() {
        return this.tenantId ;
    }

    public String getWorkerGroupName() {
        return workerGroupName;
    }

    public void setWorkerGroupName(String workerGroupName) {
        this.workerGroupName = workerGroupName;
    }

    public String getReceivers() {
        return receivers;
    }

    public void setReceivers(String receivers) {
        this.receivers = receivers;
    }

    public String getReceiversCc() {
        return receiversCc;
    }

    public void setReceiversCc(String receiversCc) {
        this.receiversCc = receiversCc;
    }

    @Override
    public String toString() {
        return "ProcessInstance{" +
                "id=" + id +
                ", processDefinitionId=" + processDefinitionId +
                ", state=" + state +
                ", recovery=" + recovery +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", runTimes=" + runTimes +
                ", name='" + name + '\'' +
                ", host='" + host + '\'' +
                ", processDefinition=" + processDefinition +
                ", commandType=" + commandType +
                ", commandParam='" + commandParam + '\'' +
                ", taskDependType=" + taskDependType +
                ", maxTryTimes=" + maxTryTimes +
                ", failureStrategy=" + failureStrategy +
                ", warningType=" + warningType +
                ", warningGroupId=" + warningGroupId +
                ", scheduleTime=" + scheduleTime +
                ", commandStartTime=" + commandStartTime +
                ", globalParams='" + globalParams + '\'' +
                ", processInstanceJson='" + processInstanceJson + '\'' +
                ", executorId=" + executorId +
                ", tenantCode='" + tenantCode + '\'' +
                ", queue='" + queue + '\'' +
                ", isSubProcess=" + isSubProcess +
                ", locations='" + locations + '\'' +
                ", connects='" + connects + '\'' +
                ", historyCmd='" + historyCmd + '\'' +
                ", dependenceScheduleTimes='" + dependenceScheduleTimes + '\'' +
                ", duration=" + duration +
                ", processInstancePriority=" + processInstancePriority +
                ", workerGroupId=" + workerGroupId +
                ", timeout=" + timeout +
                ", tenantId=" + tenantId +
                ", workerGroupName='" + workerGroupName + '\'' +
                ", receivers='" + receivers + '\'' +
                ", receiversCc='" + receiversCc + '\'' +
                '}';
    }
}
