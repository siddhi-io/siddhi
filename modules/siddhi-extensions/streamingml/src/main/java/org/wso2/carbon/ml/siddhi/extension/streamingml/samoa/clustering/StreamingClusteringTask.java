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

import com.github.javacliparser.*;
import org.apache.samoa.learners.Learner;
import org.apache.samoa.learners.clusterers.simple.ClusteringDistributorProcessor;
import org.apache.samoa.learners.clusterers.simple.DistributedClusterer;
import org.apache.samoa.moa.cluster.Clustering;
import org.apache.samoa.streams.InstanceStream;
import org.apache.samoa.tasks.Task;
import org.apache.samoa.topology.ComponentFactory;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.topology.Topology;
import org.apache.samoa.topology.TopologyBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Queue;

/**
 * A task that runs and evaluates a distributed clustering algorithm.
 */
public class StreamingClusteringTask implements Task, Configurable {

    private static final long serialVersionUID = -8246537378371580550L;
    private static final int DISTRIBUTOR_PARALLELISM = 1;
    private static final Logger logger = LoggerFactory.getLogger(StreamingClusteringTask.class);


    public ClassOption learnerOption = new ClassOption("learner", 'l', "Clustering to run.",
            Learner.class, DistributedClusterer.class.getName());

    public ClassOption streamTrainOption = new ClassOption("streamTrain", 's', "Input stream.",
            InstanceStream.class, StreamingClusteringStream.class.getName());

    public IntOption instanceLimitOption = new IntOption("instanceLimit", 'i',
            "Maximum number of instances to test/train on  (-1 = no limit).", 100000, -1,
            Integer.MAX_VALUE);

    public StringOption evaluationNameOption = new StringOption("evaluationName", 'n',
            "Identifier of the evaluation", "Clustering__" +
            new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));


    public FloatOption samplingThresholdOption = new FloatOption("samplingThreshold", 'a',
            "Ratio of instances sampled that will be used for evaluation.", 0.5, 0.0, 1.0);
    // Default=0: no delay/waiting
    public IntOption sourceDelayOption = new IntOption("sourceDelay", 'w',
            "How many miliseconds between injections of two instances.", 0, 0, Integer.MAX_VALUE);

    private StreamingClusteringEntranceProcessor source;
    private InstanceStream streamTrain;
    private ClusteringDistributorProcessor distributor;
    private Stream distributorStream;
    private Stream evaluationStream;
    private Learner learner;
    private Topology topology;
    private TopologyBuilder builder;

    public Queue<double[]> cepEvents;
    public Queue<Clustering>samoaClusters;
    public int numberOfClusters;

    public void getDescription(StringBuilder sb) {
        sb.append("Clustering evaluation");
    }

    @Override
    public void init() {

        if (builder == null) {
            builder = new TopologyBuilder();
            builder.initTopology(evaluationNameOption.getValue(), sourceDelayOption.getValue());
            logger.debug("Successfully initializing SAMOA topology with name {}",
                    evaluationNameOption.getValue());
        }

        // instantiate ClusteringEntranceProcessor and its output stream
        source = new StreamingClusteringEntranceProcessor();
        streamTrain = this.streamTrainOption.getValue();

        if(streamTrain instanceof StreamingClusteringStream){
            logger.info("Stream is a StreamingClusteringStream");
            StreamingClusteringStream myStream = (StreamingClusteringStream)streamTrain;
            myStream.setCepEvents(this.cepEvents);
        }else{
            logger.info("Check Stream: Stream is not a StreamingClusteringStream");
        }

          // set stream to entranceProcessor
        source.setStreamSource(streamTrain);
        builder.addEntranceProcessor(source);
        source.setSamplingThreshold(samplingThresholdOption.getValue());
        source.setMaxNumInstances(instanceLimitOption.getValue());

        Stream sourceStream = builder.createStream(source);

        // distribution of instances and sampling for evaluation
        distributor = new ClusteringDistributorProcessor();
        builder.addProcessor(distributor, DISTRIBUTOR_PARALLELISM);
        builder.connectInputShuffleStream(sourceStream, distributor);
        distributorStream = builder.createStream(distributor);
        distributor.setOutputStream(distributorStream);
        evaluationStream = builder.createStream(distributor);
        distributor.setEvaluationStream(evaluationStream); // passes evaluation events along

        // instantiate learner and connect it to distributorStream
        learner = this.learnerOption.getValue();
        learner.init(builder, source.getDataset(), 1);
        builder.connectInputShuffleStream(distributorStream, learner.getInputProcessor());

        StreamingClusteringEvaluationProcessor resultCheckPoint =
                new StreamingClusteringEvaluationProcessor("Result Check Point ");
        resultCheckPoint.setSamoaClusters(this.samoaClusters);
        resultCheckPoint.setNumClusters(this.numberOfClusters);
        //resultCheckPoint,
        builder.addProcessor(resultCheckPoint);

        for (Stream evaluatorPiInputStream : learner.getResultStreams()) {
            builder.connectInputShuffleStream(evaluatorPiInputStream, resultCheckPoint);
        }
        topology = builder.build();
        logger.debug("Successfully building the topology");
        logger.info("Successfully building the topology");
    }

    @Override
    public void setFactory(ComponentFactory factory) {
        // TODO unify this code with init() for now, it's used by S4 App
        // dynamic binding theoretically will solve this problem
        builder = new TopologyBuilder(factory);
        builder.initTopology(evaluationNameOption.getValue());
        logger.debug("Successfully initialized SAMOA topology with name {}",
                evaluationNameOption.getValue());

    }

    public Topology getTopology() {
        return topology;
    }

    public void setCepEvents(Queue<double[]> cepEvents){
        this.cepEvents = cepEvents;
    }

    public void setSamoaClusters(Queue<Clustering> samoaClusters){
        this.samoaClusters = samoaClusters;
    }
    public void setNumberOfClusters(int numberOfClusters){
        this.numberOfClusters = numberOfClusters;
    }
}
