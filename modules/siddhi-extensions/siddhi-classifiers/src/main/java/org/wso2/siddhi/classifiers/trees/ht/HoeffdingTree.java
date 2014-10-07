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
package org.wso2.siddhi.classifiers.trees.ht;

import org.wso2.siddhi.classifiers.AbstractClassifier;
import org.wso2.siddhi.classifiers.UpdateableClassifier;
import org.wso2.siddhi.classifiers.trees.ht.nodes.*;
import org.wso2.siddhi.classifiers.trees.ht.utils.Utils;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.Collections;
import java.util.List;

public class HoeffdingTree extends AbstractClassifier implements
        UpdateableClassifier {
    protected Instances header;
    protected HNode rootNode;

    /** The number of instances a leaf should observe between split attempts */
    protected double gracePeriod = 200;

    /**
     * The allowable error in a split decision. Values closer to zero will take
     * longer to decide
     */
    protected double splitConfidence = 0.0000001;

    /** Threshold below which a split will be forced to break ties */
    protected double hoeffdingThreshold = 0.05;

    /**
     * The minimum fraction of weight required down at least two branches for info
     * gain splitting
     */
    protected double minFracWeightForTwoBranchesGain = 0.01;

    /** The splitting metric to use */
    protected int m_selectedSplitMetric = INFO_GAIN_SPLIT;
    protected SplitMetric m_splitMetric = new InfoGainSplitMetric(
            minFracWeightForTwoBranchesGain);

    /** The leaf prediction strategy to use */
    protected int m_leafStrategy = LEAF_NB_ADAPTIVE;

    /**
     * The number of instances (total weight) a leaf should observe before
     * allowing naive Bayes to make predictions
     */
    protected double nbThreshold = 0;

    protected int activeLeafCount;
    protected int inactiveLeafCount;
    protected int decisionNodeCount;

    public static final int GINI_SPLIT = 0;
    public static final int INFO_GAIN_SPLIT = 1;

    public static final Tag[] TAGS_SELECTION = {
            new Tag(GINI_SPLIT, "Gini split"),
            new Tag(INFO_GAIN_SPLIT, "Info gain split") };

    public static final int LEAF_MAJ_CLASS = 0;
    public static final int LEAF_NB = 1;
    public static final int LEAF_NB_ADAPTIVE = 2;

    public static final Tag[] TAGS_SELECTION2 = {
            new Tag(LEAF_MAJ_CLASS, "Majority class"),
            new Tag(LEAF_NB, "Naive Bayes"),
            new Tag(LEAF_NB_ADAPTIVE, "Naive Bayes adaptive") };



    protected void reset() {
        rootNode = null;

        activeLeafCount = 0;
        inactiveLeafCount = 0;
        decisionNodeCount = 0;
    }


    /**
     * Builds the classifier.
     *
     * @param data the data to train with
     * @throws Exception if classifier can't be built successfully
     */
    public void buildClassifier(Instances data) throws Exception {
        reset();

        header = new Instances(data, 0);
        if (m_selectedSplitMetric == GINI_SPLIT) {
            m_splitMetric = new GiniSplitMetric();
        } else {
            m_splitMetric = new InfoGainSplitMetric(minFracWeightForTwoBranchesGain);
        }

        data = new Instances(data);
        data.deleteWithMissingClass();
        for (int i = 0; i < data.numInstances(); i++) {
            updateClassifier(data.instance(i));
        }
    }

    /**
     * Updates the classifier with the given instance.
     *
     * @param inst the new training instance to include in the model
     * @exception Exception if the instance could not be incorporated in the
     *              model.
     */
    public void updateClassifier(Instance inst) throws Exception {

        if (inst.classIsMissing()) {
            return;
        }

        if (rootNode == null) {
            rootNode = newLearningNode();
        }

        LeafNode l = rootNode.leafForInstance(inst, null, null);
        HNode actualNode = l.actualNode;
        if (actualNode == null) {
            actualNode = new ActiveHNode();
            l.parentNode.setChild(l.parentBranch, actualNode);
        }

        if (actualNode instanceof LearningNode) {
            actualNode.updateNode(inst);

            if (/* m_growthAllowed && */actualNode instanceof ActiveHNode) {
                double totalWeight = actualNode.totalWeight();
                if (totalWeight
                        - ((ActiveHNode) actualNode).weightSeenAtLastSplitEval > gracePeriod) {

                    // try a split
                    trySplit((ActiveHNode) actualNode, l.parentNode, l.parentBranch);

                    ((ActiveHNode) actualNode).weightSeenAtLastSplitEval = totalWeight;
                }
            }
        }
    }
    /**
     * Create a new learning node (either majority class, naive Bayes or naive
     * Bayes adaptive)
     *
     * @return a new learning node
     * @throws Exception if a problem occurs
     */
    protected ActiveHNode newLearningNode() throws Exception {
        return new NBNode(header, nbThreshold);
    }

    /**
     * Try a split from the supplied node
     *
     * @param node the node to split
     * @param parent the parent of the node
     * @param parentBranch the branch leading to the node
     * @throws Exception if a problem occurs
     */
    protected void trySplit(ActiveHNode node, SplitNode parent,
                            String parentBranch) throws Exception {

        // non-pure?
        if (node.numEntriesInClassDistribution() > 1) {
            List<SplitCandidate> bestSplits = node.getPossibleSplits(m_splitMetric);
            Collections.sort(bestSplits);

            boolean doSplit = false;
            if (bestSplits.size() < 2) {
                doSplit = bestSplits.size() > 0;
            } else {
                // compute the Hoeffding bound
                double metricMax = m_splitMetric.getMetricRange(node.weightedClassDist);
                double hoeffdingBound = computeHoeffdingBound(metricMax,
                        splitConfidence, node.totalWeight());

                SplitCandidate best = bestSplits.get(bestSplits.size() - 1);
                SplitCandidate secondBest = bestSplits.get(bestSplits.size() - 2);

                if (best.splitMerit - secondBest.splitMerit > hoeffdingBound
                        || hoeffdingBound < hoeffdingThreshold) {
                    doSplit = true;
                }

                // TODO - remove poor attributes stuff?
            }

            if (doSplit) {
                SplitCandidate best = bestSplits.get(bestSplits.size() - 1);

                if (best.splitTest == null) {
                    // preprune
                    deactivateNode(node, parent, parentBranch);
                } else {
                    SplitNode newSplit = new SplitNode(node.weightedClassDist,
                            best.splitTest);

                    for (int i = 0; i < best.numSplits(); i++) {
                        ActiveHNode newChild = newLearningNode();
                        newChild.weightedClassDist = best.postSplitClassDistributions
                                .get(i);
                        newChild.weightSeenAtLastSplitEval = newChild.totalWeight();
                        String branchName = "";
                        if (header.attribute(best.splitTest.splitAttributes().get(0))
                                .isNumeric()) {
                            branchName = i == 0 ? "left" : "right";
                        } else {
                            Attribute splitAtt = header.attribute(best.splitTest
                                    .splitAttributes().get(0));
                            branchName = splitAtt.value(i);
                        }
                        newSplit.setChild(branchName, newChild);
                    }

                    activeLeafCount--;
                    decisionNodeCount++;
                    activeLeafCount += best.numSplits();

                    if (parent == null) {
                        rootNode = newSplit;
                    } else {
                        parent.setChild(parentBranch, newSplit);
                    }
                }
            }
        }
    }


    /**
     * Get the number of instances (weight) a leaf should observe before allowing
     * naive Bayes to make predictions
     *
     * @return the number/weight of instances
     */
    public double getNaiveBayesPredictionThreshold() {
        return nbThreshold;
    }

    protected static double computeHoeffdingBound(double max, double confidence,
                                                  double weight) {
        return Math.sqrt(((max * max) * Math.log(1.0 / confidence))
                / (2.0 * weight));
    }

    /**
     * Deactivate (prevent growth) from the supplied node
     *
     * @param toDeactivate the node to deactivate
     * @param parent the node's parent
     * @param parentBranch the branch leading to the node
     */
    protected void deactivateNode(ActiveHNode toDeactivate, SplitNode parent,
                                  String parentBranch) {
        HNode leaf = new InactiveHNode(toDeactivate.weightedClassDist);

        if (parent == null) {
            rootNode = leaf;
        } else {
            parent.setChild(parentBranch, leaf);
        }
        activeLeafCount--;
        inactiveLeafCount++;
    }
    public double[] distributionForInstance(Instance inst) throws Exception {

        Attribute classAtt = inst.classAttribute();
        double[] pred = new double[classAtt.numValues()];

        if (rootNode != null) {
            LeafNode l = rootNode.leafForInstance(inst, null, null);
            HNode actualNode = l.actualNode;

            if (actualNode == null) {
                actualNode = l.parentNode;
            }
            pred = actualNode.getDistribution(inst, classAtt);

        } else {
            // all class values equally likely
            for (int i = 0; i < classAtt.numValues(); i++) {
                pred[i] = 1;
            }
            Utils.normalize(pred);
        }

        // Utils.normalize(pred);
        return pred;
    }


}
