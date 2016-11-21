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

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.evaluation.PerformanceEvaluator;
import org.apache.samoa.instances.Utils;
import org.apache.samoa.learners.ResultContentEvent;
import org.apache.samoa.moa.core.Measurement;
import org.apache.samoa.moa.evaluation.LearningCurve;
import org.apache.samoa.moa.evaluation.LearningEvaluation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils.EvaluationProcessor;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class StreamingClassificationEvaluationProcessor extends EvaluationProcessor {

    private static final long serialVersionUID = -2778051819116753612L;
    private static final Logger logger =
            LoggerFactory.getLogger(StreamingClassificationEvaluationProcessor.class);

    private final PerformanceEvaluator evaluator;
    private final int samplingFrequency;
    private File dumpFile;
    private Queue<Vector> statistics;
    public Queue<Vector> classifiers;

    protected StreamingClassificationEvaluationProcessor
            (StreamingClassificationEvaluationProcessor.Builder builder) {

        this.immediateResultStream = null;
        this.firstDump = true;
        this.totalCount = 0L;
        this.experimentStart = 0L;
        this.sampleStart = 0L;
        this.evaluator = builder.evaluator;
        this.samplingFrequency = builder.samplingFrequency;
        this.dumpFile = builder.dumpFile;
        statistics = new ConcurrentLinkedQueue<>();
    }

    public boolean process(ContentEvent event) {
        boolean predicting = false;
        ResultContentEvent result = (ResultContentEvent) event;
        // Identify the event that uses to predict or train
        if (result.getInstance().classValue() == -1) {
            predicting = true;
        }
        // After every interval log the current statistics
        if (this.totalCount > 0L && this.totalCount % (long) this.samplingFrequency == 0L && !predicting) {
            this.addMeasurement();     //calculate measurements(Current statistics )
            if (!statistics.isEmpty()) {
                Vector stat = statistics.poll();
                logger.info(stat.toString());
            }
        }

        int prediction = Utils.maxIndex(result.getClassVotes());
        if (predicting) {
            Vector instanceValues = new Vector();
            for (int i = 0; i < result.getInstance().numValues() - 1; i++) {
                instanceValues.add(result.getInstance().value(i));
            }
            instanceValues.add(prediction);
            classifiers.add(instanceValues);
        } else {
            // Training event data added to model statistics
            this.evaluator.addResult(result.getInstance(), result.getClassVotes());
            ++this.totalCount;
        }
        if (result.isLastEvent()) {
            this.concludeMeasurement();
            return true;
        }

        if (totalCount == 1) {
            sampleStart = System.nanoTime();
            experimentStart = sampleStart;
        }
        return false;
    }

    public void onCreate(int id) {
        this.processId = id;
        this.learningCurve = new LearningCurve("evaluation instances");
        if (this.dumpFile != null) {
            try {
                if (this.dumpFile.exists()) {
                    this.immediateResultStream = new PrintStream(new FileOutputStream(this.dumpFile,
                            true), true);
                } else {
                    this.immediateResultStream = new PrintStream(new FileOutputStream(this.dumpFile)
                            , true);
                }
            } catch (FileNotFoundException var3) {
                this.immediateResultStream = null;
                throw new ExecutionPlanRuntimeException(var3);
            } catch (Exception var4) {
                this.immediateResultStream = null;
                throw new ExecutionPlanRuntimeException(var4);
            }
        }
        this.firstDump = true;
    }

    public Processor newProcessor(Processor p) {
        StreamingClassificationEvaluationProcessor originalProcessor =
                (StreamingClassificationEvaluationProcessor) p;
        StreamingClassificationEvaluationProcessor newProcessor =
                (new StreamingClassificationEvaluationProcessor.Builder(originalProcessor)).build();
        newProcessor.setSamoaClassifiers(classifiers);
        if (originalProcessor.learningCurve != null) {
            newProcessor.learningCurve = originalProcessor.learningCurve;
        }
        return newProcessor;
    }

    public String toString() {
        StringBuilder report = new StringBuilder();
        report.append(StreamingClassificationEvaluationProcessor.class.getCanonicalName());
        report.append("id = ").append(this.processId);
        report.append('\n');
        if (this.learningCurve.numEntries() > 0) {
            report.append(this.learningCurve.toString());
            report.append('\n');
        }
        return report.toString();
    }

    private void addMeasurement() {
        Vector measurements = new Vector();
        measurements.add(new Measurement("evaluation instances", (double) this.totalCount));
        Collections.addAll(measurements, this.evaluator.getPerformanceMeasurements());
        Measurement[] finalMeasurements =
                (Measurement[]) measurements.toArray(new Measurement[measurements.size()]);
        LearningEvaluation learningEvaluation = new LearningEvaluation(finalMeasurements);
        this.learningCurve.insertEntry(learningEvaluation);

        try {
            statistics.add(measurements);
        } catch (Exception e) {
            throw new ExecutionPlanRuntimeException("Fail to add measurements : ", e);
        }

        if (this.immediateResultStream != null) {
            if (this.firstDump) {
                this.immediateResultStream.println(this.learningCurve.headerToString());
                this.firstDump = false;
            }
            this.immediateResultStream.println(this.learningCurve.entryToString
                    (this.learningCurve.numEntries() - 1));
            this.immediateResultStream.flush();
        }

    }

    private void concludeMeasurement() {
        long experimentEnd = System.nanoTime();
        long totalExperimentTime = TimeUnit.SECONDS.convert(experimentEnd - this.experimentStart,
                TimeUnit.NANOSECONDS);
        logger.info("total evaluation time: {} seconds for {} instances",
                Long.valueOf(totalExperimentTime), Long.valueOf(this.totalCount));
        if (this.immediateResultStream != null) {
            this.immediateResultStream.println("# COMPLETED");
            this.immediateResultStream.flush();
        }
    }
    public void setSamoaClassifiers(Queue<Vector> classifiers) {
        this.classifiers = classifiers;
    }

    public static class Builder {
        private final PerformanceEvaluator evaluator;
        private int samplingFrequency = 100000;
        private File dumpFile = null;

        public Builder(PerformanceEvaluator evaluator) {
            this.evaluator = evaluator;
        }

        public Builder(StreamingClassificationEvaluationProcessor oldProcessor) {
            this.evaluator = oldProcessor.evaluator;
            this.samplingFrequency = oldProcessor.samplingFrequency;
            this.dumpFile = oldProcessor.dumpFile;
        }

        public StreamingClassificationEvaluationProcessor.Builder samplingFrequency
                (int samplingFrequency) {
            this.samplingFrequency = samplingFrequency;
            return this;
        }

        public StreamingClassificationEvaluationProcessor.Builder dumpFile(File file) {
            this.dumpFile = file;
            return this;
        }

        public StreamingClassificationEvaluationProcessor build() {
            return new StreamingClassificationEvaluationProcessor(this);
        }
    }
}