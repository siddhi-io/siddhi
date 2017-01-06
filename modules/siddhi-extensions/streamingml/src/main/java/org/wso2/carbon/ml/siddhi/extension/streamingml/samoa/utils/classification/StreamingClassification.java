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

package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils.classification;

import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;

import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;

public class StreamingClassification extends Thread {

    private int maxInstance;
    private int batchSize;                       //Output display interval
    private int numberOfClasses;
    private int numberOfAttributes;
    private int numberOfNominals;
    private int parallelism;
    private int bagging;
    private String nominalAttributesValues;
    public int numEventsReceived;
    public Queue<double[]> cepEvents;                            //Cep events
    public Queue<Vector> samoaClassifiers;                       // Output prediction data
    public StreamingClassificationTaskBuilder classificationTask;

    public StreamingClassification(int maxInstance, int batchSize, int classes, int paraCount,
                                   int nominals, String str, int par, int bagging) {

        this.maxInstance = maxInstance;
        this.numberOfClasses = classes;
        this.numberOfAttributes = paraCount;
        this.numberOfNominals = nominals;
        this.batchSize = batchSize;
        this.nominalAttributesValues = str;
        this.parallelism = par;
        this.bagging = bagging;
        this.numEventsReceived = 0;

        this.cepEvents = new ConcurrentLinkedQueue<double[]>();
        this.samoaClassifiers = new ConcurrentLinkedQueue<Vector>();

        try {
            this.classificationTask = new StreamingClassificationTaskBuilder(this.maxInstance,
                    this.batchSize, this.numberOfClasses, this.numberOfAttributes,
                    this.numberOfNominals,this.nominalAttributesValues, this.cepEvents,
                    this.samoaClassifiers, this.parallelism, this.bagging);
        } catch (Exception e) {
            throw new ExecutionPlanRuntimeException("Fail to Initiate the Streaming " +
                    "Classification : ", e);
        }
    }

    public void run() {
        classificationTask.initTask();
        classificationTask.submit();
    }

    public void addEvents(double[] eventData) {
        numEventsReceived++;
        cepEvents.add(eventData);
    }

    public Object[] getOutput() {
        Object[] output;
        if (!samoaClassifiers.isEmpty()) {
            output = new Object[numberOfAttributes];
            Vector prediction = samoaClassifiers.poll();
            for (int i = 0; i < prediction.size(); i++) {
                output[i] = prediction.get(i);
            }
        } else {
            output = null;
        }
        return output;
    }
}

