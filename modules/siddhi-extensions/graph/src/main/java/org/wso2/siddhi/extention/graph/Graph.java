/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.siddhi.extention.graph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * This class is used to generate the graph from the stream data
 */
public class Graph {
    private HashMap<String, Set<String>> adjacencyList = new HashMap<String, Set<String>>(10000);

    /**
     * This is used to add a vertex to the graph
     *
     * @param user vertex id
     */
    private void addVertex(String user) {
        if (!adjacencyList.containsKey(user)) {
            adjacencyList.put(user, new HashSet<String>());
        }
    }

    /**
     * This is use to create an edge between two vertices
     *
     * @param user1 vertex1
     * @param user2 vertex2
     */
    public void addEdge(String user1, String user2) {
        addVertex(user1);
        addVertex(user2);
        if (!adjacencyList.get(user1).contains(user2)) {
            adjacencyList.get(user1).add(user2);
        }
        if (!adjacencyList.get(user2).contains(user1)) {
            adjacencyList.get(user2).add(user1);
        }
    }

    /**
     * This is to check whether there is an edge between two vertices
     *
     * @param user   vertex1
     * @param vertex vertex2
     * @return true or false
     */
    public boolean existsEdge(String user, String vertex) {
        Set<String> neighbors = adjacencyList.get(user);
        if (neighbors == null) {
            return false;
        } else {
            return neighbors.contains(vertex);
        }
    }

    /**
     * This is used to get the degree of a vertex
     *
     * @param user vertex
     * @return degree of the vertex
     */
    public int getDegree(String user) {
        return adjacencyList.get(user).size();
    }

    /**
     * This is used to get the adjacency list of a vertex
     *
     * @param user vertex id
     * @return Set containing adjacent vertices
     */
    public Set<String> getNeighbors(String user) {
        return adjacencyList.get(user);
    }

    /**
     * Get the size of the adjacency list
     *
     * @return size of the adjacency list
     */
    public int size() {
        return adjacencyList.size();
    }

    /**
     * This is used to get the graph
     *
     * @return adjacency list which contain the graph
     */
    public HashMap<String, Set<String>> getGraph() {
        return adjacencyList;
    }
}
