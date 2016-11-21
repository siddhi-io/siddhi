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

import org.apache.samoa.learners.clusterers.simple.ClusteringDistributorProcessor;
import org.apache.samoa.moa.cluster.Clustering;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils.ProcessTask;

import java.util.Queue;

/**
 * A task that runs and evaluates a distributed clustering algorithm.
 */

public class StreamingClusteringTask extends ProcessTask {

    private static final int DISTRIBUTOR_PARALLELISM = 1;
    private static final Logger logger = LoggerFactory.getLogger(StreamingClusteringTask.class);

    private ClusteringDistributorProcessor distributor;
    private Stream evaluationStream;
    public Queue<Clustering> samoaClusters;
    public int numberOfClusters;

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
        inputStream = this.streamTrainOption.getValue();

        if (inputStream instanceof StreamingClusteringStream) {
            logger.info("DataStream is a StreamingClusteringStream");
            StreamingClusteringStream myStream = (StreamingClusteringStream) inputStream;
            myStream.setCepEvents(this.cepEvents);
        } else {
            logger.info("Check DataStream: DataStream is not a StreamingClusteringStream");
        }

        // set stream to entranceProcessor
        source.setStreamSource(inputStream);
        builder.addEntranceProcessor(source);
        source.setMaxInstances(instanceLimitOption.getValue());

        Stream sourceStream = builder.createStream(source);

        // distribution of instances and sampling for evaluation
        distributor = new ClusteringDistributorProcessor();
        builder.addProcessor(distributor, DISTRIBUTOR_PARALLELISM);
        builder.connectInputShuffleStream(sourceStream, distributor);
        sourcePiOutputStream = builder.createStream(distributor);
        distributor.setOutputStream(sourcePiOutputStream);
        evaluationStream = builder.createStream(distributor);
        distributor.setEvaluationStream(evaluationStream); // passes evaluation events along

        // instantiate learner and connect it to sourcePiOutputStream
        learner = this.learnerOption.getValue();
        learner.init(builder, source.getDataset(), 1);
        builder.connectInputShuffleStream(sourcePiOutputStream, learner.getInputProcessor());

        // Evaluation Processor
        StreamingClusteringEvaluationProcessor resultCheckPoint =
                new StreamingClusteringEvaluationProcessor("Result Check Point ");
        resultCheckPoint.setSamoaClusters(this.samoaClusters);
        resultCheckPoint.setNumClusters(this.numberOfClusters);
        builder.addProcessor(resultCheckPoint);

        for (Stream evaluatorPiInputStream : learner.getResultStreams()) {
            builder.connectInputShuffleStream(evaluatorPiInputStream, resultCheckPoint);
        }
        topology = builder.build();
        logger.debug("Successfully building the topology");
        logger.info("Successfully building the topology");
    }

    public void setSamoaClusters(Queue<Clustering> samoaClusters) {
        this.samoaClusters = samoaClusters;
    }

    public void setNumberOfClusters(int numberOfClusters) {
        this.numberOfClusters = numberOfClusters;
    }
}
