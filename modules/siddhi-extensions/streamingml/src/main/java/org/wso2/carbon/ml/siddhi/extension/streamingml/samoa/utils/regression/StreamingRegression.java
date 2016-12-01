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
package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils.regression;

import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;

import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;

public class StreamingRegression extends Thread {

    private int maxInstance;
    private int batchSize;            //Output display interval
    private int numberOfAttributes;
    private int parallelism;
    public int numEventsReceived;

    public Queue<double[]> cepEvents;
    public Queue<Vector> samoaPredictions;
    public StreamingRegressionTaskBuilder regressionTask;

    public StreamingRegression(int maxInstance, int batchSize, int parameterCount, int parallelism) {

        this.maxInstance = maxInstance;
        this.numberOfAttributes = parameterCount;
        this.batchSize = batchSize;
        this.parallelism = parallelism;
        this.numEventsReceived = 0;

        this.cepEvents = new ConcurrentLinkedQueue<double[]>();
        this.samoaPredictions = new ConcurrentLinkedQueue<Vector>();
        try {
            this.regressionTask = new StreamingRegressionTaskBuilder(this.maxInstance, this.batchSize,
                    this.numberOfAttributes, this.cepEvents, this.samoaPredictions, this.parallelism);
        } catch (Exception e) {
            throw new ExecutionPlanRuntimeException("Fail to initiate the streaming regression" +
                    " task.", e);
        }
    }

    public void run() {
        regressionTask.initTask();
        regressionTask.submit();
    }

    public void addEvents(double[] cepEvent) {
        numEventsReceived++;
        cepEvents.add(cepEvent);
    }

    public Object[] getOutput() {
        Object[] output;
        if (!samoaPredictions.isEmpty()) {             // poll predicted events from prediction queue
            output = new Object[numberOfAttributes];
            Vector prediction = samoaPredictions.poll();
            for (int i = 0; i < prediction.size(); i++) {
                output[i] = prediction.get(i);
            }
        } else {
            output = null;
        }
        return output;
    }
}
