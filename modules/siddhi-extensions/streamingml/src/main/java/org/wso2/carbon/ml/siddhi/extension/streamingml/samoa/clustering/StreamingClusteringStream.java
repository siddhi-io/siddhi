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

package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.clustering;

import com.github.javacliparser.IntOption;

import org.apache.samoa.instances.*;
import org.apache.samoa.moa.core.DataPoint;
import org.apache.samoa.moa.core.Example;
import org.apache.samoa.moa.core.InstanceExample;
import org.apache.samoa.moa.core.ObjectRepository;
import org.apache.samoa.moa.tasks.TaskMonitor;
import org.apache.samoa.streams.InstanceStream;
import org.apache.samoa.streams.clustering.ClusteringStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;

public class StreamingClusteringStream extends ClusteringStream {
    private static final Logger logger = LoggerFactory.getLogger(StreamingClusteringStream.class);

    protected InstancesHeader streamHeader;
    private int numGeneratedInstances;
    LinkedList<DataPoint> points = new LinkedList<DataPoint>();

    public Queue<double[]> cepEvents;
    double[] values;
    private int numberOfAttributes;

    public IntOption numClusterOption = new IntOption("numCluster", 'K',
            "The average number of centroids in the model.", 5, 1, Integer.MAX_VALUE);

    @Override
    protected void prepareForUseImpl(TaskMonitor taskMonitor, ObjectRepository objectRepository) {
        taskMonitor.setCurrentActivity("Preparing random RBF...", -1.0);
        this.numberOfAttributes = this.numAttsOption.getValue();
        generateHeader();
        restart();
        values = new double[numberOfAttributes];

        for (int i = 0; i < numberOfAttributes; i++) {
            values[i] = 0;
        }
    }

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
    public Example<Instance> nextInstance() {
        double[] values_new = new double[numAttsOption.getValue()];
        if (numGeneratedInstances == 0) {
            while (cepEvents == null) ;
        }
        numGeneratedInstances++;
        while (cepEvents.isEmpty()) ;
        double[] values = cepEvents.poll();
        System.arraycopy(values, 0, values_new, 0, values.length);
        Instance inst = new DenseInstance(1.0, values_new);
        inst.setDataset(getHeader());
        return new InstanceExample(inst);
    }

    @Override
    public boolean isRestartable() {
        return true;
    }

    @Override
    public void restart() {
        numGeneratedInstances = 0;
    }

    @Override
    public void getDescription(StringBuilder stringBuilder, int i) {

    }

    protected void generateHeader() {
        ArrayList<Attribute> attributes = new ArrayList<Attribute>();
        for (int i = 0; i < this.numAttsOption.getValue(); i++) {
            attributes.add(new Attribute("att" + (i + 1)));
        }

        ArrayList<String> classLabels = new ArrayList<String>();
        for (int i = 0; i < this.numClusterOption.getValue(); i++) {
            classLabels.add("class" + (i + 1));
        }

        attributes.add(new Attribute("class", classLabels));
        streamHeader = new InstancesHeader(new Instances(getCLICreationString(InstanceStream.class),
                attributes, 0));
        streamHeader.setClassIndex(streamHeader.numAttributes() - 1);
    }

    public void setCepEvents(Queue<double[]> cepEvents) {
        this.cepEvents = cepEvents;
    }
}
