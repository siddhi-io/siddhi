/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/
package org.wso2.siddhi.classifiers.utils;

import org.wso2.siddhi.classifiers.trees.ht.HoeffdingTree;
import org.wso2.siddhi.classifiers.trees.ht.Instance;
import org.wso2.siddhi.classifiers.trees.ht.Instances;
import org.wso2.siddhi.classifiers.trees.ht.utils.Utils;


public class Evaluation {
    /**
     * The number of classes.
     */
    protected int classCount; // currently only support 1

    /**
     * The weight of all incorrectly classified instances.
     */
    protected double inCorrectResults;

    /**
     * The weight of all correctly classified instances.
     */
    protected double correctResults;

    /**
     * The weight of all unclassified instances.
     */
    protected double unClassified;

    /**
     * The weight of all instances that had no class assigned to them.
     */
    protected double missingClass;

    /**
     * The weight of all instances that had a class assigned to them.
     */
    protected double withClass;

    /**
     * Array for storing the confusion matrix.
     */
    protected double[][] confusionMatrix;

    /**
     * Is the class nominal or numeric?
     */
    protected boolean m_ClassIsNominal;

    /**
     * The prior probabilities of the classes.
     */
    protected double[] classPriors;

    /**
     * The sum of counts for priors.
     */
    protected double classPriorsSum;

    /**
     * Total Kononenko & Bratko Information.
     */
    protected double sumKBInfo;

    /**
     * Resolution of the margin histogram.
     */
    protected static int marginResolution = 500;

    /**
     * Cumulative margin distribution.
     */
    protected double marginCounts[];

    /**
     * Number of non-missing class training instances seen.
     */
    protected int numTrainClassVals;

    /**
     * Array containing all numeric training class values seen.
     */
    protected double[] trainClassVals;

    /**
     * Array containing all numeric training class weights.
     */
    protected double[] trainClassWeights;


    /**
     * The minimum probablility accepted from an estimator to avoid taking log(0)
     * in Sf calculations.
     */
    protected static final double MIN_SF_PROB = Double.MIN_VALUE;

    /**
     * Total entropy of prior predictions.
     */
    protected double sumPriorEntropy;

    /**
     * Total entropy of scheme predictions.
     */
    protected double sumSchemeEntropy;

    /**
     * The confidence level used for coverage statistics.
     */
    protected double confLevel = 0.95;

    /**
     * Total size of predicted regions at the given confidence level.
     */
    protected double totalSizeOfRegions;

    /**
     * Total coverage of test cases at the given confidence level.
     */
    protected double totalCoverage;

    /**
     * Minimum target value.
     */
    protected double minTarget;

    /**
     * Maximum target value.
     */
    protected double maxTarget;


    /**
     * enables/disables the use of priors, e.g., if no training set is present in
     * case of de-serialized schemes.
     */
    protected boolean noPriors = false;

    protected Instances m_Header;

    protected String[] m_ClassNames;


    public Evaluation(Instances data) {
        m_Header = new Instances(data, 0);
        classCount = data.numClasses();
        confusionMatrix = new double[classCount][classCount];
        m_ClassNames = new String[classCount];
        classPriors = new double[classCount];
        setPriors(data);
        marginCounts = new double[marginResolution + 1];
    }

    /**
     * Sets the class prior probabilities.
     *
     * @param train the training instances used to determine the prior
     *              probabilities
     * @throws Exception if the class attribute of the instances is not set
     */
    public void setPriors(Instances train) {
        noPriors = false;
        numTrainClassVals = 0;
        trainClassVals = null;
        trainClassWeights = null;
        minTarget = Double.MAX_VALUE;
        maxTarget = -Double.MAX_VALUE;

        for (int i = 0; i < train.numInstances(); i++) {
            Instance currentInst = train.instance(i);
            if (!currentInst.classIsMissing()) {
                addNumericTrainClass(currentInst.classValue(), currentInst.weight());
            }
        }

        classPriors[0] = classPriorsSum = 0;
        for (int i = 0; i < train.numInstances(); i++) {
            if (!train.instance(i).classIsMissing()) {
                classPriors[0] += train.instance(i).classValue()
                        * train.instance(i).weight();
                classPriorsSum += train.instance(i).weight();
            }
        }
    }

    protected void addNumericTrainClass(double classValue, double weight) {

        // Update minimum and maximum target value
        if (classValue > maxTarget) {
            maxTarget = classValue;
        }
        if (classValue < minTarget) {
            minTarget = classValue;
        }

        // Update buffer
        if (trainClassVals == null) {
            trainClassVals = new double[100];
            trainClassWeights = new double[100];
        }
        if (numTrainClassVals == trainClassVals.length) {
            double[] temp = new double[trainClassVals.length * 2];
            System.arraycopy(trainClassVals, 0, temp, 0, trainClassVals.length);
            trainClassVals = temp;

            temp = new double[trainClassWeights.length * 2];
            System.arraycopy(trainClassWeights, 0, temp, 0,
                    trainClassWeights.length);
            trainClassWeights = temp;
        }
        trainClassVals[numTrainClassVals] = classValue;
        trainClassWeights[numTrainClassVals] = weight;
        numTrainClassVals++;
    }

    public double evaluationForSingleInstance(HoeffdingTree classifier,
                                                 Instance instance) throws Exception {
        Instance classMissing = (Instance) instance.copy();
        classMissing.setDataset(instance.dataset());
        classMissing.setClassMissing();
        // System.out.println("instance (to predict)" + classMissing);
        double pred = evaluationForSingleInstance(
                classifier.distributionForInstance(classMissing), instance);
        return pred;
    }

    public double evaluationForSingleInstance(double[] dist, Instance instance) throws Exception {
        double pred;
        pred = Utils.maxIndex(dist);
        if (dist[(int) pred] <= 0) {
            pred = Utils.missingValue();
        }
        updateStatsForClassifier(dist, instance);
        return pred;
    }

    protected void updateMargins(double[] predictedDistribution, int actualClass,
                                 double weight) {

        double probActual = predictedDistribution[actualClass];
        double probNext = 0;

        for (int i = 0; i < classCount; i++) {
            if ((i != actualClass) && (predictedDistribution[i] > probNext)) {
                probNext = predictedDistribution[i];
            }
        }

        double margin = probActual - probNext;
        int bin = (int) ((margin + 1.0) / 2.0 * marginResolution);
        marginCounts[bin] += weight;
    }

    protected void updateStatsForClassifier(double[] predictedDistribution,
                                            Instance instance) throws Exception {

        int actualClass = (int) instance.classValue();

        if (!instance.classIsMissing()) {
            updateMargins(predictedDistribution, actualClass, instance.weight());

            // Determine the predicted class (doesn't detect multiple
            // classifications)
            int predictedClass = -1;
            double bestProb = 0.0;
            for (int i = 0; i < classCount; i++) {
                if (predictedDistribution[i] > bestProb) {
                    predictedClass = i;
                    bestProb = predictedDistribution[i];
                }
            }

            withClass += instance.weight();
            // Update counts when no class was predicted
            if (predictedClass < 0) {
                unClassified += instance.weight();
                return;
            }

            double predictedProb = Math.max(MIN_SF_PROB,
                    predictedDistribution[actualClass]);
            double priorProb = Math.max(MIN_SF_PROB, classPriors[actualClass]
                    / classPriorsSum);
            if (predictedProb >= priorProb) {
                sumKBInfo += (Utils.log2(predictedProb) - Utils.log2(priorProb))
                        * instance.weight();
            } else {
                sumKBInfo -= (Utils.log2(1.0 - predictedProb) - Utils
                        .log2(1.0 - priorProb)) * instance.weight();
            }

            sumSchemeEntropy -= Utils.log2(predictedProb) * instance.weight();
            sumPriorEntropy -= Utils.log2(priorProb) * instance.weight();


            // Update coverage stats
            int[] indices = Utils.stableSort(predictedDistribution);
            double sum = 0, sizeOfRegions = 0;
            for (int i = predictedDistribution.length - 1; i >= 0; i--) {
                if (sum >= confLevel) {
                    break;
                }
                sum += predictedDistribution[indices[i]];
                sizeOfRegions++;
                if (actualClass == indices[i]) {
                    totalCoverage += instance.weight();
                }
            }
            totalSizeOfRegions += sizeOfRegions / (maxTarget - minTarget);

            // Update other stats
            confusionMatrix[actualClass][predictedClass] += instance.weight();
            if (predictedClass != actualClass) {
                inCorrectResults += instance.weight();
            } else {
                correctResults += instance.weight();
            }
        } else {
            missingClass += instance.weight();
        }
    }
}
