/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.siddhi.extension.largestClique;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Created on 8/5/16.
 */
public class Graph {
    private HashMap<Long, Set<Long>> adjacencyList = new HashMap<Long, Set<Long>>(10000);

    public void addVertex(long user) {
        if (!adjacencyList.containsKey(user)) {
            adjacencyList.put(user, new HashSet<Long>());
        }
    }

    public void addEdge(long user1, long user2) {
        if (!adjacencyList.containsKey(user1) || !adjacencyList.containsKey(user2)) {
            System.err.println("Error: Cannot add edge, because users do not exist");
        }
        if (!adjacencyList.get(user1).contains(user2)) {
            adjacencyList.get(user1).add(user2);
        }
        if (!adjacencyList.get(user2).contains(user1)) {
            adjacencyList.get(user2).add(user1);
        }
    }

    public boolean existsEdge(long u, long v) {
        Set<Long> neighbors = adjacencyList.get(u);
        if (neighbors == null) {
            return false;
        } else {
            return neighbors.contains(v);
        }
    }

    public int getDegree(long u) {
        return adjacencyList.get(u).size();
    }

    public Set<Long> getNeighbors(long u) {
        return adjacencyList.get(u);
    }

    public int size() {
        return adjacencyList.size();
    }

    public HashMap<Long, Set<Long>> getGraph() {
        return adjacencyList;
    }
}

