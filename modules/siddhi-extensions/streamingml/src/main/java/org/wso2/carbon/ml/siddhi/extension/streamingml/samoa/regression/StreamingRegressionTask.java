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

package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.regression;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.samoa.evaluation.BasicClassificationPerformanceEvaluator;
import org.apache.samoa.evaluation.BasicRegressionPerformanceEvaluator;
import org.apache.samoa.evaluation.ClassificationPerformanceEvaluator;
import org.apache.samoa.evaluation.PerformanceEvaluator;
import org.apache.samoa.evaluation.RegressionPerformanceEvaluator;
import org.apache.samoa.learners.ClassificationLearner;
import org.apache.samoa.learners.Learner;
import org.apache.samoa.learners.RegressionLearner;
import org.apache.samoa.learners.classifiers.rules.VerticalAMRulesRegressor;
import org.apache.samoa.tasks.Task;
import org.apache.samoa.streams.InstanceStream;
import org.apache.samoa.topology.ComponentFactory;
import org.apache.samoa.topology.Stream;
import org.apache.samoa.topology.Topology;
import org.apache.samoa.topology.TopologyBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.javacliparser.ClassOption;
import com.github.javacliparser.Configurable;
import com.github.javacliparser.FileOption;
import com.github.javacliparser.IntOption;
import com.github.javacliparser.StringOption;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;

public class StreamingRegressionTask implements Task, Configurable {

    private static final long serialVersionUID = -8246537378371580550L;
    private static Logger logger = LoggerFactory.getLogger(StreamingRegression.class);

    public ClassOption learnerOption = new ClassOption("learner", 'l', "Learner.", Learner.class,
            VerticalAMRulesRegressor.class.getName());

    public ClassOption streamTrainOption = new ClassOption("trainStream", 's',
            "Stream to learn from.", InstanceStream.class, StreamingRegressionStream.class.getName());

    public ClassOption evaluatorOption = new ClassOption("evaluator", 'e',
            "Classification performance evaluation method.", PerformanceEvaluator.class,
            BasicRegressionPerformanceEvaluator.class.getName());

    public IntOption instanceLimitOption = new IntOption("instanceLimit", 'i',
            "Maximum number of instances to test/train on  (-1 = no limit).", 1000000, -1,
            Integer.MAX_VALUE);

    public IntOption timeLimitOption = new IntOption("timeLimit", 't', "Maximum number of seconds" +
            " to test/train for (-1 = no limit).", -1, -1, Integer.MAX_VALUE);

    public IntOption sampleFrequencyOption = new IntOption("sampleFrequency", 'f', "How many" +
            " instances between samples of the learning performance.", 100000, 0, Integer.MAX_VALUE);

    public StringOption evaluationNameOption = new StringOption("evaluationName", 'n',
            "Identifier of the evaluation", "Prequential_" +
            new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));

    public FileOption dumpFileOption = new FileOption("dumpFile", 'd', "File to append" +
            " intermediate csv results to", null, "csv", true);

    // Default=0: no delay/waiting
    public IntOption sourceDelayOption = new IntOption("sourceDelay", 'w',
            "How many microseconds between injections of two instances.", 0, 0, Integer.MAX_VALUE);

    // Batch size to delay the incoming stream: delay of x milliseconds after each batch
    public IntOption batchDelayOption = new IntOption("delayBatchSize", 'b', "The delay batch " +
            "size: delay of x milliseconds after each batch ", 1, 1, Integer.MAX_VALUE);

    protected StreamingRegressionEntranceProcessor preqSource;      // EntranceProcessor
    private InstanceStream streamTrain;                             // Input stream
    protected Stream sourcePiOutputStream;                          //Result stream
    private Learner reggressionLearner;                             // VerticalAMRulesRegressor
    private StreamingRegressionEvaluationProcessor evaluator;
    protected Topology prequentialTopology;
    protected TopologyBuilder builder;

    public Queue<double[]> cepEvents;
    public Queue<Vector> samoaPredictions;

    @Override
    public void init() {
        streamTrain = this.streamTrainOption.getValue();

        if (streamTrain instanceof StreamingRegressionStream) {
            StreamingRegressionStream myStream = (StreamingRegressionStream) streamTrain;
            myStream.setCepEvent(this.cepEvents);
        } else {
            throw new ExecutionPlanRuntimeException("Check stream: " +
                    "Stream is not a StreamingRegressionStream");
        }

        if (builder == null) {
            builder = new TopologyBuilder();
            builder.initTopology(evaluationNameOption.getValue());
            logger.debug("Successfully initializing SAMOA topology with name {}",
                    evaluationNameOption.getValue());
        }

        // instantiate PrequentialSourceProcessor and its output stream
        preqSource = new StreamingRegressionEntranceProcessor();

        preqSource.setStreamSource(streamTrain);
        builder.addEntranceProcessor(preqSource);
        preqSource.setMaxNumInstances(instanceLimitOption.getValue());
        preqSource.setSourceDelay(sourceDelayOption.getValue());
        preqSource.setDelayBatchSize(batchDelayOption.getValue());

        sourcePiOutputStream = builder.createStream(preqSource);

        // instantiate classifier and connect it to sourcePiOutputStream
        reggressionLearner = this.learnerOption.getValue();
        reggressionLearner.init(builder, preqSource.getDataset(), 1);
        builder.connectInputShuffleStream(sourcePiOutputStream,
                reggressionLearner.getInputProcessor());

        PerformanceEvaluator evaluatorOptionValue = this.evaluatorOption.getValue();
        if (!StreamingRegressionTask.isLearnerAndEvaluatorCompatible(reggressionLearner,
                evaluatorOptionValue)) {
            evaluatorOptionValue = getDefaultPerformanceEvaluatorForLearner(reggressionLearner);
        }
        evaluator = new StreamingRegressionEvaluationProcessor.Builder(evaluatorOptionValue)
                .samplingFrequency(sampleFrequencyOption.getValue()).
                        dumpFile(dumpFileOption.getFile()).build();

        evaluator.setSamoaPredictions(samoaPredictions);
        builder.addProcessor(evaluator);
        for (Stream evaluatorPiInputStream : reggressionLearner.getResultStreams()) {
            builder.connectInputShuffleStream(evaluatorPiInputStream, evaluator);
        }
        prequentialTopology = builder.build();
        logger.debug("Successfully building the topology");
        logger.info("Successfully building the topology");
    }

    @Override
    public void setFactory(ComponentFactory factory) {
        builder = new TopologyBuilder(factory);
        builder.initTopology(evaluationNameOption.getValue());
        logger.debug("Successfully initializing SAMOA topology with name {}",
                evaluationNameOption.getValue());

    }

    public Topology getTopology() {
        return prequentialTopology;
    }

    protected static boolean isLearnerAndEvaluatorCompatible(Learner learner,
                                                             PerformanceEvaluator evaluator) {
        return (learner instanceof RegressionLearner &&
                evaluator instanceof RegressionPerformanceEvaluator) ||
                (learner instanceof ClassificationLearner &&
                        evaluator instanceof ClassificationPerformanceEvaluator);
    }

    protected static PerformanceEvaluator getDefaultPerformanceEvaluatorForLearner(Learner learner) {
        if (learner instanceof RegressionLearner) {
            return new BasicRegressionPerformanceEvaluator();
        }
        // Default to BasicClassificationPerformanceEvaluator for all other cases
        return new BasicClassificationPerformanceEvaluator();
    }

    public void setCepEvents(Queue<double[]> cepEvents) {
        this.cepEvents = cepEvents;
    }

    public void setSamoaData(Queue<Vector> data) {
        this.samoaPredictions = data;
    }
}
