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

package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils.clustering;

import org.apache.samoa.moa.cluster.Cluster;
import org.apache.samoa.moa.cluster.Clustering;

import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;


public class StreamingClustering extends Thread {
    private int numberOfAttributes;
    private int numberOfClusters;
    public int maxInstance;
    public int numEventsReceived = 0;

    public Queue<double[]> cepEvents;
    public Queue<Clustering> samoaClusters;

    public StreamingClusteringTaskBuilder clusteringTask;

    public StreamingClustering(int maxInstant, int paramCount, int numberOfClusters) {
        this.maxInstance = maxInstant;
        this.numberOfAttributes = paramCount;
        this.numberOfClusters = numberOfClusters;

        this.cepEvents = new ConcurrentLinkedQueue<double[]>();
        this.samoaClusters = new ConcurrentLinkedQueue<Clustering>();

        try {
            this.clusteringTask = new StreamingClusteringTaskBuilder(this.maxInstance,
                    this.numberOfAttributes, this.numberOfClusters,
                    this.cepEvents, this.samoaClusters);
        } catch (Exception e) {
            throw new ExecutionPlanRuntimeException("Fail to Initiate the Streaming clustering task",
                    e);
        }
    }

    public void run() {
        this.clusteringTask.initTask();
        this.clusteringTask.submit();
    }

    public void addEvents(double[] eventData) {
        numEventsReceived++;
        cepEvents.add(eventData);
    }

    public Object[] getOutput() {
        Object[] output;
        if (!samoaClusters.isEmpty()) {
            output = new Object[numberOfClusters];
            Clustering clusters = samoaClusters.poll();
            for (int i = 0; i < numberOfClusters; i++) {
                Cluster cluster = clusters.get(i);
                String centerStr = "";
                double[] center = cluster.getCenter();
                centerStr += center[0];
                for (int j = 1; j < numberOfAttributes; j++) {
                    centerStr += ("," + center[j]);
                }
                output[i] = centerStr;
            }
        } else {
            output = null;
        }
        return output;
    }

}
