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
package org.apache.dolphinscheduler.common.graph;

import org.apache.dolphinscheduler.common.utils.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * analysis of DAG
 * Node: node                                    String             任务名
 * NodeInfo：node description information        TaskNode           任务
 * EdgeInfo: edge description information        TaskNodeRelation   任务依赖关系
 */
public class DAG<Node, NodeInfo, EdgeInfo> {


  private static final Logger logger = LoggerFactory.getLogger(DAG.class);

  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * node map, key is node, value is node information
   */
  private volatile Map<Node, NodeInfo> nodesMap;

  /**
   * edge map. key is node of origin;value is Map with key for destination node and value for edge
   */
  private volatile Map<Node, Map<Node, EdgeInfo>> edgesMap;

  /**
   * reversed edge set，key is node of destination, value is Map with key for origin node and value for edge
   */
  private volatile Map<Node, Map<Node, EdgeInfo>> reverseEdgesMap;


  public DAG() {
    nodesMap = new HashMap<>();  //<节点名(任务名)，具体节点(具体任务)>
    edgesMap = new HashMap<>();  //< 原节点,< 目标节点,具体的边 > >
    reverseEdgesMap = new HashMap<>();  // < 目标节点,< 原节点,具体的边 > >
  }


  /**
   * 添加节点信息
   * add node information
   * @param node          node
   * @param nodeInfo      node information
   */
  public void addNode(Node node, NodeInfo nodeInfo) {
    lock.writeLock().lock();

    try{
      nodesMap.put(node, nodeInfo);
    }finally {
      lock.writeLock().unlock();
    }

  }


  /**
   * 添加边
   * add edge
   * @param fromNode node of origin
   * @param toNode   node of destination
   * @return The result of adding an edge. returns false if the DAG result is a ring result
   */
  public boolean addEdge(Node fromNode, Node toNode) {
    return addEdge(fromNode, toNode, false);
  }


  /**
   * add edge
   * @param fromNode        node of origin
   * @param toNode          node of destination
   * @param createNode      whether the node needs to be created if it does not exist
   * @return The result of adding an edge. returns false if the DAG result is a ring result
   */
  private boolean addEdge(Node fromNode, Node toNode, boolean createNode) {
    return addEdge(fromNode, toNode, null, createNode);
  }


  /**
   * 添加边，真正执行的
   * add edge
   * @param fromNode        node of origin
   * @param toNode          node of destination
   * @param edge            edge description
   * @param createNode      whether the node needs to be created if it does not exist
   * @return The result of adding an edge. returns false if the DAG result is a ring result
   */
  public boolean addEdge(Node fromNode, Node toNode, EdgeInfo edge, boolean createNode) {
    lock.writeLock().lock();

    try{

      // Whether an edge can be successfully added(fromNode -> toNode)
      if (!isLegalAddEdge(fromNode, toNode, createNode)) { //校验是否有环，是从目标节点一直往后找，看是否能找到原节点
        logger.error("serious error: add edge({} -> {}) is invalid, cause cycle！", fromNode, toNode);
        return false;
      }

      addNodeIfAbsent(fromNode, null);
      addNodeIfAbsent(toNode, null);

      addEdge(fromNode, toNode, edge, edgesMap);  //往 edgesMap(正向) 中添加；
      addEdge(toNode, fromNode, edge, reverseEdgesMap); //往 reverseEdgesMap(反向) 中添加；

      return true;
    }finally {
      lock.writeLock().unlock();
    }

  }


  /**
   * whether this node is contained
   *
   * @param node node
   * @return true if contains
   */
  public boolean containsNode(Node node) {
    lock.readLock().lock();

    try{
      return nodesMap.containsKey(node);
    }finally {
      lock.readLock().unlock();
    }
  }


  /**
   * whether this edge is contained
   *
   * @param fromNode node of origin
   * @param toNode   node of destination
   * @return true if contains
   */
  public boolean containsEdge(Node fromNode, Node toNode) {
    lock.readLock().lock();
    try{
      Map<Node, EdgeInfo> endEdges = edgesMap.get(fromNode);
      if (endEdges == null) {
        return false;
      }

     return endEdges.containsKey(toNode);
    }finally {
      lock.readLock().unlock();
    }
  }


  /**
   * get node description
   *
   * @param node node
   * @return node description
   */
  public NodeInfo getNode(Node node) {
    lock.readLock().lock();

    try{
      return nodesMap.get(node);
    }finally {
      lock.readLock().unlock();
    }
  }


  /**
   * Get the number of nodes
   *
   * @return the number of nodes
   */
  public int getNodesCount() {
    lock.readLock().lock();

    try{
      return nodesMap.size();
    }finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get the number of edges
   *
   * @return the number of edges
   */
  public int getEdgesCount() {
    lock.readLock().lock();
    try{
      int count = 0;

      for (Map.Entry<Node, Map<Node, EdgeInfo>> entry : edgesMap.entrySet()) {
        count += entry.getValue().size();
      }

      return count;
    }finally {
      lock.readLock().unlock();
    }
  }


  /**
   * 返回dag中的起始节点；
   * get the start node of DAG
   * @return the start node of DAG
   */
  public Collection<Node> getBeginNode() {
    lock.readLock().lock();

    try{
      return CollectionUtils.subtract(nodesMap.keySet(), reverseEdgesMap.keySet());
    }finally {
      lock.readLock().unlock();
    }

  }


  /**
   * get the end node of DAG
   *
   * @return the end node of DAG
   */
  public Collection<Node> getEndNode() {

    lock.readLock().lock();

    try{
      return CollectionUtils.subtract(nodesMap.keySet(), edgesMap.keySet());
    }finally {
      lock.readLock().unlock();
    }

  }


  /**
   * Gets all previous nodes of the node
   *
   * @param node node id to be calculated
   * @return all previous nodes of the node
   */
  public Set<Node> getPreviousNodes(Node node) {
    lock.readLock().lock();

    try{
      return getNeighborNodes(node, reverseEdgesMap);
    }finally {
      lock.readLock().unlock();
    }
  }


  /**
   * 找某个节点的全部后续节点，按图一直往后找吗，所有？ 错，只往后找了一层；
   * Get all subsequent nodes of the node
   * @param node node id to be calculated
   * @return all subsequent nodes of the node
   */
  public Set<Node> getSubsequentNodes(Node node) {
    lock.readLock().lock();

    try{
      return getNeighborNodes(node, edgesMap); //往后找一层
    }finally {
      lock.readLock().unlock();
    }
  }


  /**
   * Gets the degree of entry of the node
   *
   * @param node node id
   * @return the degree of entry of the node
   */
  public int getIndegree(Node node) {
    lock.readLock().lock();

    try{
      return getPreviousNodes(node).size();
    }finally {
      lock.readLock().unlock();
    }
  }


  /**
   * whether the graph has a ring
   *
   * @return true if has cycle, else return false.
   */
  public boolean hasCycle() {
    lock.readLock().lock();
    try{
        return !topologicalSortImpl().getKey();
    }finally {
      lock.readLock().unlock();
    }
  }


  /**
   * Only DAG has a topological sort
   * @return topologically sorted results, returns false if the DAG result is a ring result
   * @throws Exception errors
   */
  public List<Node> topologicalSort() throws Exception {
    lock.readLock().lock();

    try{
      Map.Entry<Boolean, List<Node>> entry = topologicalSortImpl();

      if (entry.getKey()) {
        return entry.getValue();
      }

      throw new Exception("serious error: graph has cycle ! ");
    }finally {
      lock.readLock().unlock();
    }
  }


  /**
   *  if tho node does not exist,add this node
   *
   * @param node    node
   * @param nodeInfo node information
   */
  private void addNodeIfAbsent(Node node, NodeInfo nodeInfo) {
    if (!containsNode(node)) {
      addNode(node, nodeInfo);
    }
  }


  /**
   * add edge   往  reverseEdgesMap  /  edgeMap 中添加；
   *
   * @param fromNode node of origin
   * @param toNode   node of destination
   * @param edge  edge description
   * @param edges edge set
   */
  private void addEdge(Node fromNode, Node toNode, EdgeInfo edge, Map<Node, Map<Node, EdgeInfo>> edges) {
    edges.putIfAbsent(fromNode, new HashMap<>());
    Map<Node, EdgeInfo> toNodeEdges = edges.get(fromNode);
    toNodeEdges.put(toNode, edge);
  }


  /**
   * 校验新加的边是否会导致环，
   * 实现的思路就是：是从目标节点一直往后找，看是否能找到原节点
   *
   * Whether an edge can be successfully added(fromNode -> toNode)
   * need to determine whether the DAG has cycle
   * @param fromNode     node of origin
   * @param toNode       node of destination
   * @param createNode whether to create a node
   * @return true if added
   */
  private boolean isLegalAddEdge(Node fromNode, Node toNode, boolean createNode) {
      if (fromNode.equals(toNode)) { //开始节点和结束节点不能一样
          logger.error("edge fromNode({}) can't equals toNode({})", fromNode, toNode);
          return false;
      }

      if (!createNode) {
          if (!containsNode(fromNode) || !containsNode(toNode)){ //开始节点和结束节点必须存在
              logger.error("edge fromNode({}) or toNode({}) is not in vertices map", fromNode, toNode);
              return false;
          }
      }

      // Whether an edge can be successfully added(fromNode -> toNode),need to determine whether the DAG has cycle!
      int verticesCount = getNodesCount(); //节点个数

      Queue<Node> queue = new LinkedList<>();

      queue.add(toNode); //从往后往前？错，是从目标节点往后找，看是否能找到原节点

      // if DAG doesn't find fromNode, it's not has cycle!
      while (!queue.isEmpty() && (--verticesCount > 0)) {
          Node key = queue.poll();

          for (Node subsequentNode : getSubsequentNodes(key)) {  // 找当前节点的后续节点，只找了一层
              if (subsequentNode.equals(fromNode)) { //后续有一个节点和 源节点相等了，说明有环
                  return false;
              }

              queue.add(subsequentNode);//又加到队列中，也就是一直往后找，直到队列为空了；
          }
      }

      return true;
  }


  /**
   * 某个节点的 "邻居" 节点，可以往前找，也可以往后找，但是只找一层
   * Get all neighbor nodes of the node
   *
   * @param node   Node id to be calculated
   * @param edges neighbor edge information
   * @return all neighbor nodes of the node
   */
  private Set<Node> getNeighborNodes(Node node, final Map<Node, Map<Node, EdgeInfo>> edges) {
    final Map<Node, EdgeInfo> neighborEdges = edges.get(node); //这个节点后续连接的节点； 就找了一层啊；

    if (neighborEdges == null) {
      return Collections.EMPTY_MAP.keySet();
    }

    return neighborEdges.keySet();
  }



  /**
   * Determine whether there are ring and topological sorting results
   *
   * Directed acyclic graph (DAG) has topological ordering
   * Breadth First Search：
   *    1、Traversal of all the vertices in the graph, the degree of entry is 0 vertex into the queue
   *    2、Poll a vertex in the queue to update its adjacency (minus 1) and queue the adjacency if it is 0 after minus 1
   *    3、Do step 2 until the queue is empty
   * If you cannot traverse all the nodes, it means that the current graph is not a directed acyclic graph.
   * There is no topological sort.
   *
   *
   * @return key Returns the state
   * if success (acyclic) is true, failure (acyclic) is looped,
   * and value (possibly one of the topological sort results)
   */
  private Map.Entry<Boolean, List<Node>> topologicalSortImpl() {
    // node queue with degree of entry 0
    Queue<Node> zeroIndegreeNodeQueue = new LinkedList<>();
    // save result
    List<Node> topoResultList = new ArrayList<>();
    // save the node whose degree is not 0
    Map<Node, Integer> notZeroIndegreeNodeMap = new HashMap<>();

    // Scan all the vertices and push vertexs with an entry degree of 0 to queue
    for (Map.Entry<Node, NodeInfo> vertices : nodesMap.entrySet()) {
      Node node = vertices.getKey();
      int inDegree = getIndegree(node);

      if (inDegree == 0) {
        zeroIndegreeNodeQueue.add(node);
        topoResultList.add(node);
      } else {
        notZeroIndegreeNodeMap.put(node, inDegree);
      }
    }

    /**
     * After scanning, there is no node with 0 degree of entry,
     * indicating that there is a ring, and return directly
     */
    if(zeroIndegreeNodeQueue.isEmpty()){
      return new AbstractMap.SimpleEntry(false, topoResultList);
    }

    // The topology algorithm is used to delete nodes with 0 degree of entry and its associated edges
    while (!zeroIndegreeNodeQueue.isEmpty()) {
      Node v = zeroIndegreeNodeQueue.poll();
      // Get the neighbor node
      Set<Node> subsequentNodes = getSubsequentNodes(v);

      for (Node subsequentNode : subsequentNodes) {

        Integer degree = notZeroIndegreeNodeMap.get(subsequentNode);

        if(--degree == 0){
          topoResultList.add(subsequentNode);
          zeroIndegreeNodeQueue.add(subsequentNode);
          notZeroIndegreeNodeMap.remove(subsequentNode);
        }else{
          notZeroIndegreeNodeMap.put(subsequentNode, degree);
        }

      }
    }

    // if notZeroIndegreeNodeMap is empty,there is no ring!
    AbstractMap.SimpleEntry resultMap = new AbstractMap.SimpleEntry(notZeroIndegreeNodeMap.size() == 0 , topoResultList);
    return resultMap;

  }

}

