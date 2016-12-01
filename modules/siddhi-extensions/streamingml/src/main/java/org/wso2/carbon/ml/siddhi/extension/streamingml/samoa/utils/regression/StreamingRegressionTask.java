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

import org.apache.samoa.evaluation.PerformanceEvaluator;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils.ProcessTask;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;

import java.util.Queue;
import java.util.Vector;

public class StreamingRegressionTask extends ProcessTask {

    private static Logger logger = LoggerFactory.getLogger(StreamingRegression.class);

    public Queue<Vector> samoaPredictions;

    @Override
    public void init() {
        inputStream = this.streamTrainOption.getValue();

        if (inputStream instanceof StreamingRegressionStream) {
            StreamingRegressionStream myStream = (StreamingRegressionStream) inputStream;
            myStream.setCepEvents(this.cepEvents);
        } else {
            throw new ExecutionPlanRuntimeException("Check stream: " +
                    "DataStream is not a StreamingRegressionStream");
        }

        if (builder == null) {
            builder = new TopologyBuilder();
            builder.initTopology(evaluationNameOption.getValue());
            logger.debug("Successfully initialized SAMOA topology with name {}",
                    evaluationNameOption.getValue());
        }

        // instantiate PrequentialSourceProcessor and its output stream
        source = new StreamingRegressionEntranceProcessor();

        source.setStreamSource(inputStream);
        builder.addEntranceProcessor(source);
        source.setMaxInstances(instanceLimitOption.getValue());
        source.setSourceDelay(sourceDelayOption.getValue());
        source.setDelayBatchSize(batchDelayOption.getValue());

        sourcePiOutputStream = builder.createStream(source);

        // instantiate learner and connect it to sourcePiOutputStream
        learner = this.learnerOption.getValue();
        learner.init(builder, source.getDataset(), 1);
        builder.connectInputShuffleStream(sourcePiOutputStream,
                learner.getInputProcessor());

        PerformanceEvaluator evaluatorOptionValue = this.evaluatorOption.getValue();
        if (!StreamingRegressionTask.isLearnerAndEvaluatorCompatible(learner,
                evaluatorOptionValue)) {
            evaluatorOptionValue = getDefaultPerformanceEvaluatorForLearner(learner);
        }
        StreamingRegressionEvaluationProcessor evaluator =
                new StreamingRegressionEvaluationProcessor.Builder(evaluatorOptionValue)
                .samplingFrequency(sampleFrequencyOption.getValue()).
                        dumpFile(dumpFileOption.getFile()).build();

        evaluator.setSamoaPredictions(samoaPredictions);
        builder.addProcessor(evaluator);
        for (Stream evaluatorPiInputStream : learner.getResultStreams()) {
            builder.connectInputShuffleStream(evaluatorPiInputStream, evaluator);
        }
        topology = builder.build();
        logger.info("Successfully built the topology");
    }

    public void setSamoaData(Queue<Vector> data) {
        this.samoaPredictions = data;
    }
}
