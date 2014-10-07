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

import org.wso2.siddhi.classifiers.bayes.NaiveBayes;
import org.wso2.siddhi.classifiers.bayes.NaiveBayesUpdateable;
import org.wso2.siddhi.classifiers.trees.ht.ActiveHNode;
import org.wso2.siddhi.classifiers.trees.ht.Instance;
import org.wso2.siddhi.classifiers.trees.ht.Instances;
import org.wso2.siddhi.query.api.definition.Attribute;

public class NBNode extends ActiveHNode implements LearningNode {
/*
    /** The naive Bayes model at the node */
    protected NaiveBayes bayes;

    /**
     * The weight of instances that need to be seen by this node before allowing
     * naive Bayes to make predictions
     */
    protected double naiveBayesThreshold;

    /**
     * Construct a new NBNode
     *
     * @param header the instances structure of the data we're learning from
     * @param nbWeightThreshold the weight mass to see before allowing naive Bayes
     *          to predict
     * @throws Exception if a problem occurs
     */
    public NBNode(Instances header, double nbWeightThreshold) throws Exception {
        naiveBayesThreshold = nbWeightThreshold;
        bayes = new NaiveBayesUpdateable();
        bayes.buildClassifier(header);
    }

    @Override
    public void updateNode(Instance inst) throws Exception {
        super.updateNode(inst);

        try {
            bayes.updateClassifier(inst);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public double[] getDistribution(Instance inst, Attribute classAtt)
            throws Exception {

        // totalWeight - weightSeenAtLastSplitEval is the weight mass
        // observed by this node's NB model

        boolean doNB = naiveBayesThreshold == 0 ? true : (totalWeight()
                - weightSeenAtLastSplitEval > naiveBayesThreshold);

        if (doNB) {
            return bayes.distributionForInstance(inst);
        }

        return super.getDistribution(inst,classAtt);
    }
}
