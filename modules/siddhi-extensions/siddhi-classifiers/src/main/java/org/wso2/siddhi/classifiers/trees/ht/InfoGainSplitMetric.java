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

import org.wso2.siddhi.classifiers.trees.ht.utils.Utils;

import java.util.List;
import java.util.Map;

public class InfoGainSplitMetric extends SplitMetric {
    protected double minFracWeightForTwoBranches;

    public InfoGainSplitMetric(double minFracWeightForTwoBranches) {
        this.minFracWeightForTwoBranches = minFracWeightForTwoBranches;
    }

    @Override
    public double evaluateSplit(Map<String, WeightMass> preDist,
                                List<Map<String, WeightMass>> postDist) {

        double[] pre = new double[preDist.size()];
        int count = 0;
        for (Map.Entry<String, WeightMass> e : preDist.entrySet()) {
            pre[count++] = e.getValue().weight;
        }

        double preEntropy = ContingencyTables.entropy(pre);

        double[] distWeights = new double[postDist.size()];
        double totalWeight = 0.0;
        for (int i = 0; i < postDist.size(); i++) {
            distWeights[i] = SplitMetric.sum(postDist.get(i));
            totalWeight += distWeights[i];
        }

        int fracCount = 0;
        for (double d : distWeights) {
            if (d / totalWeight > minFracWeightForTwoBranches) {
                fracCount++;
            }
        }

        if (fracCount < 2) {
            return Double.NEGATIVE_INFINITY;
        }

        double postEntropy = 0;
        for (int i = 0; i < postDist.size(); i++) {
            Map<String, WeightMass> d = postDist.get(i);
            double[] post = new double[d.size()];
            count = 0;
            for (Map.Entry<String, WeightMass> e : d.entrySet()) {
                post[count++] = e.getValue().weight;
            }
            postEntropy += distWeights[i] * ContingencyTables.entropy(post);
        }

        if (totalWeight > 0) {
            postEntropy /= totalWeight;
        }

        return preEntropy - postEntropy;
    }

    @Override
    public double getMetricRange(Map<String, WeightMass> preDist) {

        int numClasses = preDist.size();
        if (numClasses < 2) {
            numClasses = 2;
        }

        return Utils.log2(numClasses);
    }

}
