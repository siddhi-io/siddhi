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

import org.apache.samoa.evaluation.PerformanceEvaluator;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils.ProcessTask;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;

import java.util.Queue;
import java.util.Vector;

/**
 * Source : Samoa prequentialtask : https://github.com/apache/incubator-samoa/blob/master/
 * samoa-api/src/main/java/org/apache/samoa/tasks/PrequentialEvaluation.java
 */

public class StreamingClassificationTask extends ProcessTask {

    private static Logger logger = LoggerFactory.getLogger(StreamingClassificationTask.class);

    public Queue<Vector> classifiers;
    public int numClasses = 2;

    @Override
    public void init() {
        inputStream = this.streamTrainOption.getValue();
        if (inputStream instanceof StreamingClassificationStream) {
            StreamingClassificationStream myStream = (StreamingClassificationStream) inputStream;
            myStream.setCepEvents(this.cepEvents);
        } else {
            throw new ExecutionPlanRuntimeException("Check DataStream: " +
                    "DataStream is not a StreamingClassificationStream");
        }

        if (builder == null) {               // This part done by setFactory method
            builder = new TopologyBuilder();
            builder.initTopology(evaluationNameOption.getValue());
            logger.debug("Successfully initialized SAMOA topology with name {}",
                    evaluationNameOption.getValue());
        }

        source = new StreamingClassificationEntranceProcessor();

        // Set stream to Entrance processor
        source.setStreamSource(inputStream);
        source.setMaxInstances(instanceLimitOption.getValue());
        source.setSourceDelay(sourceDelayOption.getValue());
        source.setDelayBatchSize(batchDelayOption.getValue());
        builder.addEntranceProcessor(source);

        // Create stream from Entrance processor
        sourcePiOutputStream = builder.createStream(source);

        learner = this.learnerOption.getValue();   // Vertical Hoeffding Tree
        learner.init(builder, source.getDataset(), 1);
        builder.connectInputShuffleStream(sourcePiOutputStream, learner.getInputProcessor());

        // Set ClassificationPerformanceEvaluator
        PerformanceEvaluator evaluatorOptionValue = this.evaluatorOption.getValue();
        if (!StreamingClassificationTask.isLearnerAndEvaluatorCompatible(learner,
                evaluatorOptionValue)) {
            evaluatorOptionValue = getDefaultPerformanceEvaluatorForLearner(learner);
        }

        // Set ClassificationEvaluationProcessor
        StreamingClassificationEvaluationProcessor evaluator
                = new StreamingClassificationEvaluationProcessor.Builder(evaluatorOptionValue).
                samplingFrequency(sampleFrequencyOption.getValue()).
                dumpFile(dumpFileOption.getFile()).build();

        evaluator.setSamoaClassifiers(classifiers);
        builder.addProcessor(evaluator);
        for (Stream evaluatorPiInputStream : learner.getResultStreams()) {
            builder.connectInputShuffleStream(evaluatorPiInputStream, evaluator);
        }

        topology = builder.build();
        logger.info("Successfully built the topology");
    }


    public void setSamoaClassifiers(Queue<Vector> classifiers) {
        this.classifiers = classifiers;
    }

    public void setNumClasses(int numClasses) {
        this.numClasses = numClasses;
    }

}