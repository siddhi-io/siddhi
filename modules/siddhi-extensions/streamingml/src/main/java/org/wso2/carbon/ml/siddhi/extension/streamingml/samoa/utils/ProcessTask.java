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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Queue;

import com.github.javacliparser.FloatOption;
import org.apache.samoa.evaluation.BasicClassificationPerformanceEvaluator;
import org.apache.samoa.evaluation.BasicRegressionPerformanceEvaluator;
import org.apache.samoa.evaluation.ClassificationPerformanceEvaluator;
import org.apache.samoa.evaluation.PerformanceEvaluator;
import org.apache.samoa.evaluation.RegressionPerformanceEvaluator;
import org.apache.samoa.learners.ClassificationLearner;
import org.apache.samoa.learners.Learner;
import org.apache.samoa.learners.RegressionLearner;
import org.apache.samoa.learners.classifiers.trees.VerticalHoeffdingTree;
import org.apache.samoa.streams.InstanceStream;
import org.apache.samoa.tasks.Task;
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
import org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils.classification.StreamingClassificationPerformanceEvaluator;
import org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils.classification.StreamingClassificationStream;

/**
 * Source : Samoa prequentialtask : https://github.com/apache/incubator-samoa/blob/master/samoa-api
 * /src/main/java/org/apache/samoa/tasks/PrequentialEvaluation.java
 */
public abstract class ProcessTask implements Task, Configurable {

    private static final long serialVersionUID = -8246537378371580550L;
    private static Logger logger = LoggerFactory.getLogger(ProcessTask.class);

    public ClassOption learnerOption = new ClassOption("learner", 'l', "Classifier to train.",
            Learner.class, VerticalHoeffdingTree.class.getName());

    public ClassOption streamTrainOption = new ClassOption("trainStream", 's', "DataStream to learn" +
            " from.", InstanceStream.class, StreamingClassificationStream.class.getName());

    public ClassOption evaluatorOption = new ClassOption("evaluator", 'e',
            "StreamingClassification performance valuation method.", PerformanceEvaluator.class,
            StreamingClassificationPerformanceEvaluator.class.getName());

    public IntOption sampleFrequencyOption = new IntOption("sampleFrequency", 'f', "How many" +
            " instances between samples of the learning performance.", 1000, 0, Integer.MAX_VALUE);

    public StringOption evaluationNameOption = new StringOption("evaluationName", 'n',
            "Identifier of the evaluation", "Prequential_" +
            new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));

    public FileOption dumpFileOption = new FileOption("dumpFile", 'd',
            "File to append intermediate csv results to", null, "csv", true);

    public IntOption instanceLimitOption = new IntOption("instanceLimit", 'i', "Maximum number" +
            " of instances to test/train on  (-1 = no limit).", 1000000, -1, Integer.MAX_VALUE);

    // Default=0: no delay/waiting
    public IntOption sourceDelayOption = new IntOption("sourceDelay", 'w', "How many microseconds" +
            " between injections of two instances.", 0, 0, Integer.MAX_VALUE);

    // Batch size to delay the incoming stream: delay of x milliseconds after each batch
    public IntOption batchDelayOption = new IntOption("delayBatchSize", 'b', "The delay batch" +
            " size: delay of x milliseconds after each batch ", 1, 1, Integer.MAX_VALUE);

    public FloatOption samplingThresholdOption = new FloatOption("samplingThreshold", 'a',
            "Ratio of instances sampled that will be used for evaluation.", 0.5, 0.0, 1.0);

    protected SourceProcessor source;            // EntranceProcessor
    protected InstanceStream inputStream;        //InputStream
    protected Stream sourcePiOutputStream;       //OutputStream
    protected Learner learner;                   // Samoa Learner
    protected Topology topology;                 // Topology
    protected TopologyBuilder builder;
    public Queue<double[]> cepEvents;

    @Override
    public abstract void init();

    @Override
    public void setFactory(ComponentFactory factory) {
        builder = new TopologyBuilder(factory);
        builder.initTopology(evaluationNameOption.getValue());
        logger.debug("Successfully initialized SAMOA topology with name {}",
                evaluationNameOption.getValue());

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

    public Topology getTopology() {
        return topology;
    }

    public void setCepEvents(Queue<double[]> cepEvents) {
        this.cepEvents = cepEvents;
    }

}