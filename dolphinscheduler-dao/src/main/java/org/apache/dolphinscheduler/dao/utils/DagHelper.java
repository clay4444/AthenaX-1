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
package org.apache.dolphinscheduler.dao.utils;


import org.apache.dolphinscheduler.common.enums.TaskDependType;
import org.apache.dolphinscheduler.common.graph.DAG;
import org.apache.dolphinscheduler.common.model.TaskNode;
import org.apache.dolphinscheduler.common.model.TaskNodeRelation;
import org.apache.dolphinscheduler.common.process.ProcessDag;
import org.apache.dolphinscheduler.common.utils.CollectionUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.ProcessData;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * dag tools
 */
public class DagHelper {


    private static final Logger logger = LoggerFactory.getLogger(DagHelper.class);


    /**
     * 构建DAG需要的所有任务节点之间的边
     * generate flow node relation list by task node list;
     * Edges that are not in the task Node List will not be added to the result
     * @param taskNodeList taskNodeList
     * @return task node relation list
     */
    public static List<TaskNodeRelation> generateRelationListByFlowNodes(List<TaskNode> taskNodeList) {
        List<TaskNodeRelation> nodeRelationList = new ArrayList<>();
        for (TaskNode taskNode : taskNodeList) {
            String preTasks = taskNode.getPreTasks();  //也是从后往前找
            List<String> preTaskList = JSONUtils.toList(preTasks, String.class);
            if (preTaskList != null) { //有前置节点
                for (String depNodeName : preTaskList) {
                    if (null != findNodeByName(taskNodeList, depNodeName)) {
                        nodeRelationList.add(new TaskNodeRelation(depNodeName, taskNode.getName())); //添加一个边
                    }
                }
            }
        }
        return nodeRelationList;
    }

    /**
     * 生成构建dag需要的Task节点，正常还是返回了taskNodeList
     * generate task nodes needed by dag
     * @param taskNodeList taskNodeList
     * @param startNodeNameList startNodeNameList
     * @param recoveryNodeNameList recoveryNodeNameList
     * @param taskDependType taskDependType
     * @return task node list
     */
    public static List<TaskNode> generateFlowNodeListByStartNode(List<TaskNode> taskNodeList, List<String> startNodeNameList,
                                                                 List<String> recoveryNodeNameList, TaskDependType taskDependType) {
        List<TaskNode> destFlowNodeList = new ArrayList<>();
        List<String> startNodeList = startNodeNameList;  //指定的开始节点，空

        if(taskDependType != TaskDependType.TASK_POST
                && startNodeList.size() == 0){
            logger.error("start node list is empty! cannot continue run the process ");
            return destFlowNodeList;
        }
        List<TaskNode> destTaskNodeList = new ArrayList<>();
        List<TaskNode> tmpTaskNodeList = new ArrayList<>();
        if (taskDependType == TaskDependType.TASK_POST
                && recoveryNodeNameList.size() > 0) {  //如果有恢复的节点，就从这些恢复的节点开始；
            startNodeList = recoveryNodeNameList;
        }
        if (startNodeList == null || startNodeList.size() == 0) {
            // no special designation start nodes  用户没有指定起始任务
            tmpTaskNodeList = taskNodeList;
        } else {
            // specified start nodes or resume execution
            for (String startNodeName : startNodeList) {
                TaskNode startNode = findNodeByName(taskNodeList, startNodeName);
                List<TaskNode> childNodeList = new ArrayList<>();
                if (TaskDependType.TASK_POST == taskDependType) {
                    childNodeList = getFlowNodeListPost(startNode, taskNodeList);
                } else if (TaskDependType.TASK_PRE == taskDependType) {
                    childNodeList = getFlowNodeListPre(startNode, recoveryNodeNameList, taskNodeList);
                } else {
                    childNodeList.add(startNode);
                }
                tmpTaskNodeList.addAll(childNodeList);
            }
        }

        for (TaskNode taskNode : tmpTaskNodeList) {  //正常tmpTaskNodeList就是所有借点！
            if (null == findNodeByName(destTaskNodeList, taskNode.getName())) {
                destTaskNodeList.add(taskNode);
            }
        }
        return destTaskNodeList; //正常返回的还是所有任!
    }


    /**
     * find all the nodes that depended on the start node
     * @param startNode startNode
     * @param taskNodeList taskNodeList
     * @return task node list
     */
    private static List<TaskNode> getFlowNodeListPost(TaskNode startNode, List<TaskNode> taskNodeList) {
        List<TaskNode> resultList = new ArrayList<>();
        for (TaskNode taskNode : taskNodeList) {
            List<String> depList = taskNode.getDepList();
            if (depList != null) {
                if (depList.contains(startNode.getName())) {
                    resultList.addAll(getFlowNodeListPost(taskNode, taskNodeList));
                }
            }

        }
        resultList.add(startNode);
        return resultList;
    }


    /**
     * find all nodes that start nodes depend on.
     * @param startNode startNode
     * @param recoveryNodeNameList recoveryNodeNameList
     * @param taskNodeList taskNodeList
     * @return task node list
     */
    private static List<TaskNode> getFlowNodeListPre(TaskNode startNode, List<String> recoveryNodeNameList, List<TaskNode> taskNodeList) {

        List<TaskNode> resultList = new ArrayList<>();

        List<String> depList = startNode.getDepList();
        resultList.add(startNode);
        if (depList == null || depList.size() == 0) {
            return resultList;
        }
        for (String depNodeName : depList) {
            TaskNode start = findNodeByName(taskNodeList, depNodeName);
            if (recoveryNodeNameList.contains(depNodeName)) {
                resultList.add(start);
            } else {
                resultList.addAll(getFlowNodeListPre(start, recoveryNodeNameList, taskNodeList));
            }
        }
        return resultList;
    }

    /**
     * >>>>>> 核心，生成流程实例
     * generate dag by start nodes and recovery nodes
     * @param processDefinitionJson processDefinitionJson
     * @param startNodeNameList startNodeNameList
     * @param recoveryNodeNameList recoveryNodeNameList
     * @param depNodeType depNodeType
     * @return process dag
     * @throws Exception if error throws Exception
     */
    public static ProcessDag generateFlowDag(String processDefinitionJson,    //流程实例json，元数据都在这里
                                             List<String> startNodeNameList,    //从哪些任务开始启动   空
                                             List<String> recoveryNodeNameList,  //哪些任务是错误恢复的    空
                                             TaskDependType depNodeType) throws Exception {  //正常是2，即跑当前任务和后面的任务
        ProcessData processData = JSONUtils.parseObject(processDefinitionJson, ProcessData.class);  //json 转化为类

        List<TaskNode> taskNodeList = processData.getTasks(); //所有的任务

        //构建DAG需要的所有任务节点，正常还是所有的TaskNode，其他情况先跳过
        List<TaskNode> destTaskNodeList = generateFlowNodeListByStartNode(taskNodeList, startNodeNameList, recoveryNodeNameList, depNodeType);
        if (destTaskNodeList.isEmpty()) {
            return null;
        }

        //构建DAG需要的所有任务节点之间的边
        List<TaskNodeRelation> taskNodeRelations = generateRelationListByFlowNodes(destTaskNodeList);
        ProcessDag processDag = new ProcessDag();
        processDag.setEdges(taskNodeRelations); //所有的节点
        processDag.setNodes(destTaskNodeList);  //所有的边
        return processDag;
    }

    /**
     * 解析禁止执行的任务
     * parse the forbidden task nodes in process definition.
     * @param processDefinitionJson processDefinitionJson
     * @return task node map
     */
    public static Map<String, TaskNode> getForbiddenTaskNodeMaps(String processDefinitionJson){
        Map<String, TaskNode> forbidTaskNodeMap = new ConcurrentHashMap<>();
        ProcessData processData = JSONUtils.parseObject(processDefinitionJson, ProcessData.class); //json映射为类

        List<TaskNode> taskNodeList = processData.getTasks();
        for(TaskNode node : taskNodeList){
            if(node.isForbidden()){
                forbidTaskNodeMap.putIfAbsent(node.getName(), node);
            }
        }
        return forbidTaskNodeMap;
    }


    /**
     * 通过name找TaskNode
     * find node by node name
     * @param nodeDetails nodeDetails
     * @param nodeName nodeName
     * @return task node
     */
    public static TaskNode findNodeByName(List<TaskNode> nodeDetails, String nodeName) {
        for (TaskNode taskNode : nodeDetails) {
            if (taskNode.getName().equals(nodeName)) {
                return taskNode;
            }
        }
        return null;
    }


    /**
     * 从dag图中找 parentNodeName 这个节点之后的开始节点，parentNodeName为空，说明找没有前置依赖的节点
     * 涉及到了递归去找；
     *
     * get start vertex in one dag
     * it would find the post node if the start vertex is forbidden running
     * @param parentNodeName previous node
     * @param dag dag
     * @param completeTaskList completeTaskList
     * @return start Vertex list
     */
    public static Collection<String> getStartVertex(String parentNodeName, DAG<String, TaskNode, TaskNodeRelation> dag,
                                                    Map<String, TaskInstance> completeTaskList){

        if(completeTaskList == null){
            completeTaskList = new HashMap<>();
        }
        Collection<String> startVertexs = null; //dag中所有起始节点
        if(StringUtils.isNotEmpty(parentNodeName)){
            startVertexs = dag.getSubsequentNodes(parentNodeName);  //dag中找parentNodeName这个节点的后继节点
        }else{
            startVertexs = dag.getBeginNode(); //dag中找起始节点
        }

        List<String> tmpStartVertexs = new ArrayList<>();
        if(startVertexs!= null){
            tmpStartVertexs.addAll(startVertexs); //起始节点
        }

        for(String start : startVertexs){
            TaskNode startNode = dag.getNode(start);
            if(!startNode.isForbidden() && !completeTaskList.containsKey(start)){
                // the start can be submit if not forbidden and not in complete tasks
                continue;
            }
            //如果这个起始节点是禁止运行的，或者是已经完成的，那么这个节点后面的可能就需要提交了
            // then submit the post nodes
            Collection<String> postNodes = getStartVertex(start, dag, completeTaskList); //递归的找start这个节点之后的开始节点
            for(String post : postNodes){
                TaskNode postNode = dag.getNode(post);
                if(taskNodeCanSubmit(postNode, dag, completeTaskList)){ //判断这个任务是否可以提交，@TODO 这里应该不用再判断了，因为递归进去的时候，肯定已经判断了； 看能是否提个pr
                    tmpStartVertexs.add(post); //加到起始节点里；
                }
            }
            tmpStartVertexs.remove(start); //最后需要删除这个禁止运行的或者已经完成的，它不能提交运行
        }
        return tmpStartVertexs;
    }

    /**
     * 判断一个TaskNode是否可以被提交：当这个task的所有前置依赖节点都是禁止运行或者已经完成时，这个task才可以被提交
     *
     * the task can be submit when  all the depends nodes are forbidden or complete
     * @param taskNode taskNode
     * @param dag dag
     * @param completeTaskList completeTaskList
     * @return can submit
     */
    public static boolean taskNodeCanSubmit(TaskNode taskNode,
                                            DAG<String, TaskNode, TaskNodeRelation> dag,
                                            Map<String, TaskInstance> completeTaskList) {

        List<String> dependList = taskNode.getDepList();
        if(dependList == null){
            return true;
        }

        for(String dependNodeName : dependList){
            TaskNode dependNode = dag.getNode(dependNodeName);
            if(!dependNode.isForbidden() && !completeTaskList.containsKey(dependNodeName)){//依赖任务既不是禁止运行的，也不是已经完成的；
                return false; //说明这个任务不能提交
            }
        }
        return true;
    }


    /***
     * <<<<< 生成最终的三元组 DAG 视图
     * processDag 包含所有的节点，所有的边
     *
     * build dag graph
     * @param processDag processDag
     * @return dag
     */
    public static DAG<String, TaskNode, TaskNodeRelation> buildDagGraph(ProcessDag processDag) {

        DAG<String,TaskNode,TaskNodeRelation> dag = new DAG<>();

        /**
         * add vertex  添加节点
         */
        if (CollectionUtils.isNotEmpty(processDag.getNodes())){
            for (TaskNode node : processDag.getNodes()){
                dag.addNode(node.getName(),node);
            }
        }

        /**
         * add edge   添加边， 添加边的时候，会校验是否有环；
         */
        if (CollectionUtils.isNotEmpty(processDag.getEdges())){
            for (TaskNodeRelation edge : processDag.getEdges()){
                dag.addEdge(edge.getStartNode(),edge.getEndNode());
            }
        }
        return dag;
    }
}
