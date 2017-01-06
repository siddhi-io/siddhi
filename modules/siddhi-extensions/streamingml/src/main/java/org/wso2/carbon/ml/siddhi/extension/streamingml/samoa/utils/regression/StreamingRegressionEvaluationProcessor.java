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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.evaluation.PerformanceEvaluator;
import org.apache.samoa.learners.ResultContentEvent;
import org.apache.samoa.moa.core.Measurement;
import org.apache.samoa.moa.evaluation.LearningCurve;
import org.apache.samoa.moa.evaluation.LearningEvaluation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils.EvaluationProcessor;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;

public class StreamingRegressionEvaluationProcessor extends EvaluationProcessor {

    private static final Logger logger =
            LoggerFactory.getLogger(StreamingRegressionEvaluationProcessor.class);
    private static final String ORDERING_MEASUREMENT_NAME = "evaluation instances";

    private final PerformanceEvaluator evaluator;
    private final int samplingFrequency;
    private final File dumpFile;
    private transient PrintStream immediateResultStream;
    private transient boolean firstDump;
    private Queue<Vector> regressionData;
    public Queue<Vector> samoaPredictions;

    public StreamingRegressionEvaluationProcessor
            (StreamingRegressionEvaluationProcessor.Builder builder) {
        this.immediateResultStream = null;
        this.firstDump = true;
        this.totalCount = 0L;
        this.experimentStart = 0L;
        this.sampleStart = 0L;
        this.evaluator = builder.evaluator;
        this.samplingFrequency = builder.samplingFrequency;
        this.dumpFile = builder.dumpFile;
        regressionData = new ConcurrentLinkedQueue<>();
    }

    @Override
    public boolean process(ContentEvent event) {
        boolean predicting = false;
        ResultContentEvent result = (ResultContentEvent) event;
        Object a = result.getInstance().classValue();
        if (a.toString().equals("-0.0")) {
            predicting = true;
        }

        if ((totalCount > 0) && (totalCount % samplingFrequency) == 0) {
            this.addMeasurement();
            if (!regressionData.isEmpty()) {
                Vector stat = regressionData.poll();
                logger.info(stat.toString());
            }
        }

        if (result.isLastEvent()) {
            this.concludeMeasurement();
            return true;
        }
        if (!predicting) {
            evaluator.addResult(result.getInstance(), result.getClassVotes());
            totalCount += 1;
        } else {
            double prediction = (result.getClassVotes()[0]);
            Vector instanceValues = new Vector();
            for (int i = 0; i < result.getInstance().numValues() - 1; i++) {
                instanceValues.add(result.getInstance().value(i));
            }
            instanceValues.add(prediction);
            samoaPredictions.add(instanceValues);
        }

        if (totalCount == 1) {
            sampleStart = System.nanoTime();
            experimentStart = sampleStart;
        }
        return false;
    }

    @Override
    public void onCreate(int id) {
        this.processId = id;
        this.learningCurve = new LearningCurve(ORDERING_MEASUREMENT_NAME);

        if (this.dumpFile != null) {
            try {
                if (dumpFile.exists()) {
                    this.immediateResultStream = new PrintStream(
                            new FileOutputStream(dumpFile, true), true);
                } else {
                    this.immediateResultStream = new PrintStream(
                            new FileOutputStream(dumpFile), true);
                }

            } catch (FileNotFoundException e) {
                this.immediateResultStream = null;
                throw new ExecutionPlanRuntimeException("File not found exception : ", e);

            } catch (Exception e) {
                this.immediateResultStream = null;
                throw new ExecutionPlanRuntimeException(e);
            }
        }
        this.firstDump = true;
    }

    @Override
    public Processor newProcessor(Processor p) {
        StreamingRegressionEvaluationProcessor originalProcessor =
                (StreamingRegressionEvaluationProcessor) p;
        StreamingRegressionEvaluationProcessor newProcessor =
                (new StreamingRegressionEvaluationProcessor.Builder(originalProcessor)).build();
        newProcessor.setSamoaPredictions(samoaPredictions);
        if (originalProcessor.learningCurve != null) {
            newProcessor.learningCurve = originalProcessor.learningCurve;
        }
        return newProcessor;
    }



    private void addMeasurement() {
        Vector measurements = new Vector<>();
        measurements.add(new Measurement(ORDERING_MEASUREMENT_NAME, totalCount));
        Collections.addAll(measurements, evaluator.getPerformanceMeasurements());
        Measurement[] finalMeasurements = (Measurement[]) measurements.toArray(
                new Measurement[measurements.size()]);
        LearningEvaluation learningEvaluation = new LearningEvaluation(finalMeasurements);
        learningCurve.insertEntry(learningEvaluation);

        try {
            regressionData.add(measurements);
        } catch (Exception e) {
            throw new ExecutionPlanRuntimeException("Fail to add measurements : ", e);
        }

        if (immediateResultStream != null) {
            if (firstDump) {
                immediateResultStream.println(learningCurve.headerToString());
                firstDump = false;
            }
            immediateResultStream.println(
                    learningCurve.entryToString(learningCurve.numEntries() - 1));
            immediateResultStream.flush();
        }
    }

    private void concludeMeasurement() {
        logger.info("last event is received!");
        logger.info("total count: {}", this.totalCount);
        String learningCurveSummary = this.toString();
        logger.info(learningCurveSummary);
        long experimentEnd = System.nanoTime();
        long totalExperimentTime = TimeUnit.SECONDS.convert(experimentEnd - experimentStart,
                TimeUnit.NANOSECONDS);
        logger.info("total evaluation time: {} seconds for {} instances", totalExperimentTime,
                totalCount);

        if (immediateResultStream != null) {
            immediateResultStream.println("# COMPLETED");
            immediateResultStream.flush();
        }

    }

    public void setSamoaPredictions(Queue<Vector> samoaPrediction) {
        this.samoaPredictions = samoaPrediction;
    }

    public static class Builder {

        private final PerformanceEvaluator evaluator;
        private int samplingFrequency = 100000;
        private File dumpFile = null;

        public Builder(PerformanceEvaluator evaluator) {
            this.evaluator = evaluator;
        }

        public Builder(StreamingRegressionEvaluationProcessor oldProcessor) {
            this.evaluator = oldProcessor.evaluator;
            this.samplingFrequency = oldProcessor.samplingFrequency;
            this.dumpFile = oldProcessor.dumpFile;
        }

        public Builder samplingFrequency(int samplingFrequency) {
            this.samplingFrequency = samplingFrequency;
            return this;
        }

        public Builder dumpFile(File file) {
            this.dumpFile = file;
            return this;
        }

        public StreamingRegressionEvaluationProcessor build() {
            return new StreamingRegressionEvaluationProcessor(this);
        }
    }
}