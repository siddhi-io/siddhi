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

import org.apache.samoa.evaluation.ClassificationPerformanceEvaluator;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.Utils;
import org.apache.samoa.moa.AbstractMOAObject;
import org.apache.samoa.moa.core.Measurement;

import java.util.Collections;
import java.util.Vector;

/**
 * Uses for evaluate classification performance
 * Combination of Samoa  samoa/samoa-api/src/main/java/com/yahoo/labs/samoa/evaluation/BasicClassificationPerformanceEvaluator.java
 * and  samoa/samoa-api/src/main/java/com/yahoo/labs/samoa/evaluation/ClassificationPerformanceEvaluator.java
 */
public class StreamingClassificationPerformanceEvaluator extends AbstractMOAObject implements
        ClassificationPerformanceEvaluator {

    private static final long serialVersionUID = 1L;
    protected int numClasses = -1;
    protected long[] support;
    protected long[] truePos;
    protected long[] falsePos;
    protected long[] trueNeg;
    protected long[] falseNeg;

    protected double weightObserved;
    protected double weightCorrect;
    protected double[] columnKappa;
    protected double[] rowKappa;
    private double weightCorrectNoChangeClassifier;
    private int lastSeenClass;

    public void reset() {
        this.reset(this.numClasses);
    }

    public void reset(int numClasses) {
        this.numClasses = numClasses;
        this.support = new long[numClasses];
        this.truePos = new long[numClasses];
        this.falsePos = new long[numClasses];
        this.trueNeg = new long[numClasses];
        this.falseNeg = new long[numClasses];

        this.rowKappa = new double[numClasses];
        this.columnKappa = new double[numClasses];

        for (int i = 0; i < this.numClasses; ++i) {
            this.support[i] = 0L;
            this.truePos[i] = 0L;
            this.falsePos[i] = 0L;
            this.trueNeg[i] = 0L;
            this.falseNeg[i] = 0L;

            this.rowKappa[i] = 0.0D;
            this.columnKappa[i] = 0.0D;
        }

        this.weightObserved = 0.0D;
        this.weightCorrect = 0.0D;
        this.weightCorrectNoChangeClassifier = 0.0D;
        this.lastSeenClass = 0;
    }

    public void addResult(Instance inst, double[] classVotes) {
        if (this.numClasses == -1) {
            this.reset(inst.numClasses());
        }
        double weight = inst.weight();
        int trueClass = (int) inst.classValue();

        if (weight > 0.0D) {
            if (this.weightObserved == 0.0D) {
                this.reset(inst.numClasses());
            }
            this.weightObserved += weight;
            int predictedClass = Utils.maxIndex(classVotes);
            if (predictedClass == trueClass) {
                this.weightCorrect += weight;
            }
            if (this.rowKappa.length > 0) {
                this.rowKappa[predictedClass] += weight;
            }
            if (this.columnKappa.length > 0) {
                this.columnKappa[trueClass] += weight;
            }
        }

        if (this.lastSeenClass == trueClass) {
            this.weightCorrectNoChangeClassifier += weight;
        }

        this.lastSeenClass = trueClass;
        ++this.support[trueClass];
        int predictedClass = Utils.maxIndex(classVotes);

        int i;
        if (predictedClass == trueClass) {
            ++this.truePos[trueClass];

            for (i = 0; i < this.numClasses; ++i) {
                if (i != predictedClass) {
                    ++this.trueNeg[i];
                }
            }
        } else {
            ++this.falsePos[predictedClass];
            ++this.falseNeg[trueClass];

            for (i = 0; i < this.numClasses; ++i) {
                if (i != predictedClass && i != trueClass) {
                    ++this.trueNeg[i];
                }
            }
        }

    }


    public Measurement[] getPerformanceMeasurements() {
        Measurement[] statistics = new Measurement[]{new Measurement("classified instances",
                this.getTotalWeightObserved()), new Measurement("classifications correct (percent)",
                this.getFractionCorrectlyClassified() * 100.0D),
                new Measurement("Kappa Statistic (percent)",
                        this.getKappaStatistic() * 100.0D),
                new Measurement("Kappa Temporal Statistic (percent)",
                        this.getKappaTemporalStatistic() * 100.0D)};
        Vector measurements = new Vector();
        Collections.addAll(measurements, statistics);
        Collections.addAll(measurements, this.getSupportMeasurements());
        Collections.addAll(measurements, this.getPrecisionMeasurements());
        Collections.addAll(measurements, this.getRecallMeasurements());
        Collections.addAll(measurements, this.getF1Measurements());
        return (Measurement[]) measurements.toArray(new Measurement[measurements.size()]);
    }

    private Measurement[] getSupportMeasurements() {
        Measurement[] measurements = new Measurement[this.numClasses];

        for (int i = 0; i < this.numClasses; ++i) {
            String ml = String.format("class %s support", new Object[]{Integer.valueOf(i)});
            measurements[i] = new Measurement(ml, (double) this.support[i]);
        }

        return measurements;
    }

    private Measurement[] getPrecisionMeasurements() {
        Measurement[] measurements = new Measurement[this.numClasses];

        for (int i = 0; i < this.numClasses; ++i) {
            String ml = String.format("class %s precision", new Object[]{Integer.valueOf(i)});
            measurements[i] = new Measurement(ml, this.getPrecision(i), 10);
        }

        return measurements;
    }

    private Measurement[] getRecallMeasurements() {
        Measurement[] measurements = new Measurement[this.numClasses];

        for (int i = 0; i < this.numClasses; ++i) {
            String ml = String.format("class %s recall", new Object[]{Integer.valueOf(i)});
            measurements[i] = new Measurement(ml, this.getRecall(i), 10);
        }

        return measurements;
    }

    private Measurement[] getF1Measurements() {
        Measurement[] measurements = new Measurement[this.numClasses];

        for (int i = 0; i < this.numClasses; ++i) {
            String ml = String.format("class %s f1-score", new Object[]{Integer.valueOf(i)});
            measurements[i] = new Measurement(ml, this.getF1Score(i), 10);
        }
        return measurements;
    }

    public void getDescription(StringBuilder sb, int indent) {
        Measurement.getMeasurementsDescription(this.getSupportMeasurements(), sb, indent);
        Measurement.getMeasurementsDescription(this.getPrecisionMeasurements(), sb, indent);
        Measurement.getMeasurementsDescription(this.getRecallMeasurements(), sb, indent);
        Measurement.getMeasurementsDescription(this.getF1Measurements(), sb, indent);
        Measurement.getMeasurementsDescription(this.getPerformanceMeasurements(), sb, indent);
    }

    private double getPrecision(int classIndex) {
        return (double) this.truePos[classIndex] / (double) (this.truePos[classIndex] +
                this.falsePos[classIndex]);
    }

    private double getRecall(int classIndex) {
        return (double) this.truePos[classIndex] / (double) (this.truePos[classIndex] +
                this.falseNeg[classIndex]);
    }

    private double getF1Score(int classIndex) {
        double precision = this.getPrecision(classIndex);
        double recall = this.getRecall(classIndex);
        return 2.0D * precision * recall / (precision + recall);
    }

    public double getTotalWeightObserved() {
        return this.weightObserved;
    }

    public double getFractionCorrectlyClassified() {
        return this.weightObserved > 0.0D ? this.weightCorrect / this.weightObserved : 0.0D;
    }

    public double getFractionIncorrectlyClassified() {
        return 1.0D - this.getFractionCorrectlyClassified();
    }

    public double getKappaStatistic() {
        if (this.weightObserved <= 0.0D) {
            return 0.0D;
        } else {
            double p0 = this.getFractionCorrectlyClassified();
            double pc = 0.0D;

            for (int i = 0; i < this.numClasses; ++i) {
                pc += this.rowKappa[i] / this.weightObserved *
                        (this.columnKappa[i] / this.weightObserved);
            }

            return (p0 - pc) / (1.0D - pc);
        }
    }

    public double getKappaTemporalStatistic() {
        if (this.weightObserved > 0.0D) {
            double p0 = this.weightCorrect / this.weightObserved;
            double pc = this.weightCorrectNoChangeClassifier / this.weightObserved;
            return (p0 - pc) / (1.0D - pc);
        } else {
            return 0.0D;
        }
    }

}
