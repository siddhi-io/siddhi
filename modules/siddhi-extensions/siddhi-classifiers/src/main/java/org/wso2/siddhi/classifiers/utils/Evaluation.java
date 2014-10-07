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

import java.util.ArrayList;
import java.util.List;

public class Evaluation {
    /**
     * The number of classes.
     */
    protected int m_NumClasses;

    /**
     * The number of folds for a cross-validation.
     */
    protected int m_NumFolds;

    /**
     * The weight of all incorrectly classified instances.
     */
    protected double m_Incorrect;

    /**
     * The weight of all correctly classified instances.
     */
    protected double m_Correct;

    /**
     * The weight of all unclassified instances.
     */
    protected double m_Unclassified;

    /**
     * The weight of all instances that had no class assigned to them.
     */
    protected double m_MissingClass;

    /**
     * The weight of all instances that had a class assigned to them.
     */
    protected double m_WithClass;

    /**
     * Array for storing the confusion matrix.
     */
    protected double[][] m_ConfusionMatrix;

    /**
     * Is the class nominal or numeric?
     */
    protected boolean m_ClassIsNominal;

    /**
     * The prior probabilities of the classes.
     */
    protected double[] m_ClassPriors;

    /**
     * The sum of counts for priors.
     */
    protected double m_ClassPriorsSum;

    /**
     * The total cost of predictions (includes instance weights).
     */
    protected double m_TotalCost;

    /**
     * Sum of errors.
     */
    protected double m_SumErr;

    /**
     * Sum of absolute errors.
     */
    protected double m_SumAbsErr;

    /**
     * Sum of squared errors.
     */
    protected double m_SumSqrErr;

    /**
     * Sum of class values.
     */
    protected double m_SumClass;

    /**
     * Sum of squared class values.
     */
    protected double m_SumSqrClass;

    /**
     * Sum of predicted values.
     */
    protected double m_SumPredicted;

    /**
     * Sum of squared predicted values.
     */
    protected double m_SumSqrPredicted;

    /**
     * Sum of predicted * class values.
     */
    protected double m_SumClassPredicted;

    /**
     * Sum of absolute errors of the prior.
     */
    protected double m_SumPriorAbsErr;

    /**
     * Sum of absolute errors of the prior.
     */
    protected double m_SumPriorSqrErr;

    /**
     * Total Kononenko & Bratko Information.
     */
    protected double m_SumKBInfo;

    /**
     * Resolution of the margin histogram.
     */
    protected static int k_MarginResolution = 500;

    /**
     * Cumulative margin distribution.
     */
    protected double m_MarginCounts[];

    /**
     * Number of non-missing class training instances seen.
     */
    protected int m_NumTrainClassVals;

    /**
     * Array containing all numeric training class values seen.
     */
    protected double[] m_TrainClassVals;

    /**
     * Array containing all numeric training class weights.
     */
    protected double[] m_TrainClassWeights;


    /**
     * Whether complexity statistics are available.
     */
    protected boolean m_ComplexityStatisticsAvailable = true;

    /**
     * The minimum probablility accepted from an estimator to avoid taking log(0)
     * in Sf calculations.
     */
    protected static final double MIN_SF_PROB = Double.MIN_VALUE;

    /**
     * Total entropy of prior predictions.
     */
    protected double m_SumPriorEntropy;

    /**
     * Total entropy of scheme predictions.
     */
    protected double m_SumSchemeEntropy;

    /**
     * Whether coverage statistics are available.
     */
    protected boolean m_CoverageStatisticsAvailable = true;

    /**
     * The confidence level used for coverage statistics.
     */
    protected double m_ConfLevel = 0.95;

    /**
     * Total size of predicted regions at the given confidence level.
     */
    protected double m_TotalSizeOfRegions;

    /**
     * Total coverage of test cases at the given confidence level.
     */
    protected double m_TotalCoverage;

    /**
     * Minimum target value.
     */
    protected double m_MinTarget;

    /**
     * Maximum target value.
     */
    protected double m_MaxTarget;


    /**
     * enables/disables the use of priors, e.g., if no training set is present in
     * case of de-serialized schemes.
     */
    protected boolean m_NoPriors = false;


    /**
     * whether to discard predictions (and save memory).
     */
    protected boolean m_DiscardPredictions;


    /**
     * The list of metrics to display in the output
     */
    protected List<String> m_metricsToDisplay = new ArrayList<String>();


    public static final String[] BUILT_IN_EVAL_METRICS = {"Correct",
            "Incorrect", "Kappa", "Total cost", "Average cost", "KB relative",
            "KB information", "Correlation", "Complexity 0", "Complexity scheme",
            "Complexity improvement", "MAE", "RMSE", "RAE", "RRSE", "Coverage",
            "Region size", "TP rate", "FP rate", "Precision", "Recall", "F-measure",
            "MCC", "ROC area", "PRC area"};

    protected Instances m_Header;

    protected String[] m_ClassNames;


    public Evaluation(Instances data) {
        m_Header = new Instances(data, 0);
        m_NumClasses = data.numClasses();
        m_ConfusionMatrix = new double[m_NumClasses][m_NumClasses];
        m_ClassNames = new String[m_NumClasses];
        m_ClassPriors = new double[m_NumClasses];
        setPriors(data);
        m_MarginCounts = new double[k_MarginResolution + 1];
    }

    /**
     * Sets the class prior probabilities.
     *
     * @param train the training instances used to determine the prior
     *              probabilities
     * @throws Exception if the class attribute of the instances is not set
     */
    public void setPriors(Instances train) {
        m_NoPriors = false;
        m_NumTrainClassVals = 0;
        m_TrainClassVals = null;
        m_TrainClassWeights = null;
        m_MinTarget = Double.MAX_VALUE;
        m_MaxTarget = -Double.MAX_VALUE;

        for (int i = 0; i < train.numInstances(); i++) {
            Instance currentInst = train.instance(i);
            if (!currentInst.classIsMissing()) {
                addNumericTrainClass(currentInst.classValue(), currentInst.weight());
            }
        }

        m_ClassPriors[0] = m_ClassPriorsSum = 0;
        for (int i = 0; i < train.numInstances(); i++) {
            if (!train.instance(i).classIsMissing()) {
                m_ClassPriors[0] += train.instance(i).classValue()
                        * train.instance(i).weight();
                m_ClassPriorsSum += train.instance(i).weight();
            }
        }
    }

    protected void addNumericTrainClass(double classValue, double weight) {

        // Update minimum and maximum target value
        if (classValue > m_MaxTarget) {
            m_MaxTarget = classValue;
        }
        if (classValue < m_MinTarget) {
            m_MinTarget = classValue;
        }

        // Update buffer
        if (m_TrainClassVals == null) {
            m_TrainClassVals = new double[100];
            m_TrainClassWeights = new double[100];
        }
        if (m_NumTrainClassVals == m_TrainClassVals.length) {
            double[] temp = new double[m_TrainClassVals.length * 2];
            System.arraycopy(m_TrainClassVals, 0, temp, 0, m_TrainClassVals.length);
            m_TrainClassVals = temp;

            temp = new double[m_TrainClassWeights.length * 2];
            System.arraycopy(m_TrainClassWeights, 0, temp, 0,
                    m_TrainClassWeights.length);
            m_TrainClassWeights = temp;
        }
        m_TrainClassVals[m_NumTrainClassVals] = classValue;
        m_TrainClassWeights[m_NumTrainClassVals] = weight;
        m_NumTrainClassVals++;
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

        for (int i = 0; i < m_NumClasses; i++) {
            if ((i != actualClass) && (predictedDistribution[i] > probNext)) {
                probNext = predictedDistribution[i];
            }
        }

        double margin = probActual - probNext;
        int bin = (int) ((margin + 1.0) / 2.0 * k_MarginResolution);
        m_MarginCounts[bin] += weight;
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
            for (int i = 0; i < m_NumClasses; i++) {
                if (predictedDistribution[i] > bestProb) {
                    predictedClass = i;
                    bestProb = predictedDistribution[i];
                }
            }

            m_WithClass += instance.weight();
            // Update counts when no class was predicted
            if (predictedClass < 0) {
                m_Unclassified += instance.weight();
                return;
            }

            double predictedProb = Math.max(MIN_SF_PROB,
                    predictedDistribution[actualClass]);
            double priorProb = Math.max(MIN_SF_PROB, m_ClassPriors[actualClass]
                    / m_ClassPriorsSum);
            if (predictedProb >= priorProb) {
                m_SumKBInfo += (Utils.log2(predictedProb) - Utils.log2(priorProb))
                        * instance.weight();
            } else {
                m_SumKBInfo -= (Utils.log2(1.0 - predictedProb) - Utils
                        .log2(1.0 - priorProb)) * instance.weight();
            }

            m_SumSchemeEntropy -= Utils.log2(predictedProb) * instance.weight();
            m_SumPriorEntropy -= Utils.log2(priorProb) * instance.weight();


            // Update coverage stats
            int[] indices = Utils.stableSort(predictedDistribution);
            double sum = 0, sizeOfRegions = 0;
            for (int i = predictedDistribution.length - 1; i >= 0; i--) {
                if (sum >= m_ConfLevel) {
                    break;
                }
                sum += predictedDistribution[indices[i]];
                sizeOfRegions++;
                if (actualClass == indices[i]) {
                    m_TotalCoverage += instance.weight();
                }
            }
            m_TotalSizeOfRegions += sizeOfRegions / (m_MaxTarget - m_MinTarget);

            // Update other stats
            m_ConfusionMatrix[actualClass][predictedClass] += instance.weight();
            if (predictedClass != actualClass) {
                m_Incorrect += instance.weight();
            } else {
                m_Correct += instance.weight();
            }
        } else {
            m_MissingClass += instance.weight();
        }
    }
}
