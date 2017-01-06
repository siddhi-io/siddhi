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

package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils;

import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.InstancesHeader;
import org.apache.samoa.moa.core.Example;
import org.apache.samoa.moa.core.ObjectRepository;
import org.apache.samoa.moa.tasks.TaskMonitor;
import org.apache.samoa.streams.clustering.ClusteringStream;

import java.util.Queue;

public abstract class DataStream extends ClusteringStream {

    protected InstancesHeader streamHeader;
    protected int numberOfGeneratedInstances;
    protected int numberOfAttributes;
    public Queue<double[]> cepEvents;
    protected double[] values;

    protected abstract void prepareForUseImpl(TaskMonitor taskMonitor,
                                              ObjectRepository objectRepository);

    protected abstract void generateHeader();

    @Override
    public abstract Example<Instance> nextInstance();

    @Override
    public InstancesHeader getHeader() {
        return streamHeader;
    }

    @Override
    public long estimatedRemainingInstances() {
        return -1;
    }

    @Override
    public boolean hasMoreInstances() {
        return true;
    }

    @Override
    public boolean isRestartable() {
        return true;
    }

    @Override
    public void restart() {
        numberOfGeneratedInstances = 0;
    }

    @Override
    public void getDescription(StringBuilder stringBuilder, int i) {
    }

    public void setCepEvents(Queue<double[]> cepEvents) {
        this.cepEvents = cepEvents;
    }
}
