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

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.evaluation.ClusteringResultContentEvent;
import org.apache.samoa.learners.clusterers.ClusteringContentEvent;
import org.apache.samoa.moa.cluster.Clustering;
import org.apache.samoa.moa.clusterers.clustream.WithKmeans;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils.EvaluationProcessor;

import java.util.Queue;

public class StreamingClusteringEvaluationProcessor extends EvaluationProcessor {

    private static final Logger logger =
            LoggerFactory.getLogger(StreamingClusteringEvaluationProcessor.class);

    String evaluationPoint;
    public Queue<Clustering> samoaClusters;
    public int numberOfClusters;

    StreamingClusteringEvaluationProcessor(String evaluationPoint) {
        this.evaluationPoint = evaluationPoint;
    }

    @Override
    public boolean process(ContentEvent event) {
        if (event instanceof ClusteringContentEvent) {
            logger.info(event.getKey() + " " + evaluationPoint + "ClusteringContentEvent");
        } else if (event instanceof ClusteringResultContentEvent) {
            ClusteringResultContentEvent resultEvent = (ClusteringResultContentEvent) event;
            Clustering clustering = resultEvent.getClustering();

            Clustering kmeansClustering = WithKmeans.kMeans_rand(numberOfClusters, clustering);
            logger.info("K-mean Clusters: " + kmeansClustering.size() + " with dimension of : "
                    + kmeansClustering.dimension());
            //Adding samoa Clusters into class
            samoaClusters.add(kmeansClustering);
            logger.info("Number of Kernel Clusters : " + numberOfClusters +
                    " Number of k-Means Clusters:" + kmeansClustering.size());
        }
        return true;
    }

    @Override
    public void onCreate(int id) {
        this.processId = id;
        logger.info("Creating PrequentialSourceProcessor with processId {}", processId);
    }

    @Override
    public Processor newProcessor(Processor p) {
        StreamingClusteringEvaluationProcessor newEvaluator = (StreamingClusteringEvaluationProcessor) p;
        return newEvaluator;
    }

    public void setSamoaClusters(Queue<Clustering> samoaClusters) {
        this.samoaClusters = samoaClusters;
    }

    public void setNumberOfClusters(int numberOfClusters) {
        this.numberOfClusters = numberOfClusters;
    }

}
