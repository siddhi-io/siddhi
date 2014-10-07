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
package org.wso2.siddhi.classifiers.trees.ht.nodes;

import org.wso2.siddhi.classifiers.bayes.NaiveBayesUpdateable;
import org.wso2.siddhi.classifiers.trees.ht.ActiveHNode;
import org.wso2.siddhi.classifiers.trees.ht.Instances;

public class NBNode extends ActiveHNode implements LearningNode {

    /** The naive Bayes model at the node */
    protected NaiveBayesUpdateable m_bayes;

    /**
     * The weight of instances that need to be seen by this node before allowing
     * naive Bayes to make predictions
     */
    protected double m_nbWeightThreshold;

    /**
     * Construct a new NBNode
     *
     * @param header the instances structure of the data we're learning from
     * @param nbWeightThreshold the weight mass to see before allowing naive Bayes
     *          to predict
     * @throws Exception if a problem occurs
     */
    public NBNode(Instances header, double nbWeightThreshold) throws Exception {
        m_nbWeightThreshold = nbWeightThreshold;
        m_bayes = new NaiveBayesUpdateable();
        m_bayes.buildClassifier(header);
    }

    @Override
    public void updateNode(Instance inst) throws Exception {
        super.updateNode(inst);

        try {
            m_bayes.updateClassifier(inst);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected double[] bypassNB(Instance inst, Attribute classAtt)
            throws Exception {
        return super.getDistribution(inst, classAtt);
    }

    @Override
    public double[] getDistribution(Instance inst, Attribute classAtt)
            throws Exception {

        // totalWeight - m_weightSeenAtLastSplitEval is the weight mass
        // observed by this node's NB model

        boolean doNB = m_nbWeightThreshold == 0 ? true : (totalWeight()
                - m_weightSeenAtLastSplitEval > m_nbWeightThreshold);

        if (doNB) {
            return m_bayes.distributionForInstance(inst);
        }

        return super.getDistribution(inst, classAtt);
    }

    @Override
    protected int dumpTree(int depth, int leafCount, StringBuffer buff) {
        leafCount = super.dumpTree(depth, leafCount, buff);

        buff.append(" NB" + m_leafNum);

        return leafCount;
    }

    @Override
    protected void printLeafModels(StringBuffer buff) {
        buff.append("NB" + m_leafNum).append("\n").append(m_bayes.toString());
    }
}
