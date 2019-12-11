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
import lombok.Data;
import org.apache.dolphinscheduler.common.enums.FailureStrategy;
import org.apache.dolphinscheduler.common.enums.Priority;
import org.apache.dolphinscheduler.common.enums.ReleaseState;
import org.apache.dolphinscheduler.common.enums.WarningType;

import java.util.Date;

/**
 * schedule
 * 和之前的一样，每次触发，产生一次调度， 不对
 * 它代表的是 一个流程实例，按照何种方式进行调度，所以每个流程实例，对应一个scheduler，每次scheduler触发的时候，产生一个流程实例
 */
@Data
@TableName("t_ds_schedules")
public class Schedule {

  @TableId(value="id", type=IdType.AUTO)
  private int id;
  /**
   * process definition id
   */
  private int processDefinitionId;   //流程定义

  /**
   * process definition name
   */
  @TableField(exist = false)
  private String processDefinitionName;

  /**
   * project name
   */
  @TableField(exist = false)
  private String projectName;  //属于哪个项目

  /**
   * schedule description
   */
  @TableField(exist = false)
  private String definitionDescription;  //

  /**
   * schedule start time
   */
  private Date startTime;

  /**
   * schedule end time
   */
  private Date endTime;

  /**
   * crontab expression
   */
  private String crontab;  //crontab 表达式

  /**
   * failure strategy
   */
  private FailureStrategy failureStrategy;   //失败策略， end ?  continue ?

  /**
   * warning type
   */
  private WarningType warningType;  //报警类型?   失败报警? 成功报警?  全部报警?

  /**
   * create time
   */
  private Date createTime;

  /**
   * update time
   */
  private Date updateTime;

  /**
   * created user id
   */
  private int userId; //创建人

  /**
   * created user name
   */
  @TableField(exist = false)
  private String userName;

  /**
   * release state
   */
  private ReleaseState releaseState;   //上线还是下线

  /**
   * warning group id
   */
  private int warningGroupId;   //报警组


  /**
   * process instance priority
   */
  private Priority processInstancePriority;   //流程实例的优先级  0，1，2，3，4 分五级

  /**
   *  worker group id
   */
  private int workerGroupId;   //worker 组

  public int getWarningGroupId() {
    return warningGroupId;
  }

  public void setWarningGroupId(int warningGroupId) {
    this.warningGroupId = warningGroupId;
  }



  public Schedule() {
  }

  public String getProjectName() {
    return projectName;
  }

  public void setProjectName(String projectName) {
    this.projectName = projectName;
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

  public String getCrontab() {
    return crontab;
  }

  public void setCrontab(String crontab) {
    this.crontab = crontab;
  }

  public FailureStrategy getFailureStrategy() {
    return failureStrategy;
  }

  public void setFailureStrategy(FailureStrategy failureStrategy) {
    this.failureStrategy = failureStrategy;
  }

  public WarningType getWarningType() {
    return warningType;
  }

  public void setWarningType(WarningType warningType) {
    this.warningType = warningType;
  }

  public Date getCreateTime() {
    return createTime;
  }

  public void setCreateTime(Date createTime) {
    this.createTime = createTime;
  }


  public ReleaseState getReleaseState() {
    return releaseState;
  }

  public void setReleaseState(ReleaseState releaseState) {
    this.releaseState = releaseState;
  }



  public int getProcessDefinitionId() {
    return processDefinitionId;
  }

  public void setProcessDefinitionId(int processDefinitionId) {
    this.processDefinitionId = processDefinitionId;
  }

  public String getProcessDefinitionName() {
    return processDefinitionName;
  }

  public void setProcessDefinitionName(String processDefinitionName) {
    this.processDefinitionName = processDefinitionName;
  }

  public Date getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(Date updateTime) {
    this.updateTime = updateTime;
  }

  public int getUserId() {
    return userId;
  }

  public void setUserId(int userId) {
    this.userId = userId;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
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
    return "Schedule{" +
            "id=" + id +
            ", processDefinitionId=" + processDefinitionId +
            ", processDefinitionName='" + processDefinitionName + '\'' +
            ", projectName='" + projectName + '\'' +
            ", description='" + definitionDescription + '\'' +
            ", startTime=" + startTime +
            ", endTime=" + endTime +
            ", crontab='" + crontab + '\'' +
            ", failureStrategy=" + failureStrategy +
            ", warningType=" + warningType +
            ", createTime=" + createTime +
            ", updateTime=" + updateTime +
            ", userId=" + userId +
            ", userName='" + userName + '\'' +
            ", releaseState=" + releaseState +
            ", warningGroupId=" + warningGroupId +
            ", processInstancePriority=" + processInstancePriority +
            ", workerGroupId=" + workerGroupId +
            '}';
  }

  public String getDefinitionDescription() {
    return definitionDescription;
  }

  public void setDefinitionDescription(String definitionDescription) {
    this.definitionDescription = definitionDescription;
  }
}
