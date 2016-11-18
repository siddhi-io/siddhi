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


import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.EntranceProcessor;
import org.apache.samoa.core.Processor;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.Instances;
import org.apache.samoa.learners.clusterers.ClusteringContentEvent;
import org.apache.samoa.moa.core.DataPoint;
import org.apache.samoa.moa.options.AbstractOptionHandler;
import org.apache.samoa.streams.InstanceStream;
import org.apache.samoa.streams.StreamSource;
import org.apache.samoa.streams.clustering.ClusteringStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingClusteringEntranceProcessor implements EntranceProcessor {

    private static final long serialVersionUID = 4169053337917578558L;
    private static final Logger logger =
            LoggerFactory.getLogger(StreamingClusteringEntranceProcessor.class);

    private StreamSource streamSource;
    private Instance firstInstance;
    private boolean isInited = false;
    private double samplingThreshold;
    private int numberInstances;
    private int numInstanceSent = 0;
    private int groundTruthSamplingFrequency;

    @Override
    public boolean process(ContentEvent event) {
        // TODO: possible refactor of the super-interface implementation
        // of source processor does not need this method
        return false;
    }

    @Override
    public ContentEvent nextEvent() {
        groundTruthSamplingFrequency =
                ((ClusteringStream) streamSource.getStream()).getDecayHorizon();
        // FIXME should it be takend from the ClusteringEvaluation -f option instead?
        if (isFinished()) {
            // send ending event
            ClusteringContentEvent contentEvent = new ClusteringContentEvent(-1, firstInstance);
            contentEvent.setLast(true);
            return contentEvent;
        } else {
            DataPoint nextDataPoint = new DataPoint(nextInstance(), numInstanceSent);
            numInstanceSent++;
            ClusteringContentEvent contentEvent = new ClusteringContentEvent(numInstanceSent,
                    nextDataPoint);
            return contentEvent;
        }
    }

    @Override
    public void onCreate(int id) {
        logger.debug("Creating ClusteringSourceProcessor with id {}", id);
    }

    @Override
    public Processor newProcessor(Processor p) {
        StreamingClusteringEntranceProcessor newProcessor =
                new StreamingClusteringEntranceProcessor();
        StreamingClusteringEntranceProcessor originProcessor =
                (StreamingClusteringEntranceProcessor) p;
        if (originProcessor.getStreamSource() != null) {
            newProcessor.setStreamSource(originProcessor.getStreamSource().getStream());
        }
        return newProcessor;
    }

    @Override
    public boolean hasNext() {
        return (!isFinished());
    }

    @Override
    public boolean isFinished() {
        return (!streamSource.hasMoreInstances() ||
                (numberInstances >= 0 && numInstanceSent >= numberInstances));
    }


    public void setSamplingThreshold(double samplingThreshold) {
        this.samplingThreshold = samplingThreshold;
    }

    public StreamSource getStreamSource() {
        return streamSource;
    }

    public void setStreamSource(InstanceStream stream) {
        if (stream instanceof AbstractOptionHandler) {
            ((AbstractOptionHandler) (stream)).prepareForUse();
        }
        this.streamSource = new StreamSource(stream);
        firstInstance = streamSource.nextInstance().getData();
    }

    public Instances getDataset() {
        return firstInstance.dataset();
    }

    private Instance nextInstance() {
        if (this.isInited) {
            return streamSource.nextInstance().getData();
        } else {
            this.isInited = true;
            return firstInstance;
        }
    }

    public void setMaxNumInstances(int value) {
        numberInstances = value;
    }


    public int getGroundTruthSamplingFrequency() {
        return groundTruthSamplingFrequency;
    }

    public double getSamplingThreshold() {
        return samplingThreshold;
    }

    public void setGroundTruthSamplingFrequency(int groundTruthSamplingFrequency) {
        this.groundTruthSamplingFrequency = groundTruthSamplingFrequency;
    }

}
