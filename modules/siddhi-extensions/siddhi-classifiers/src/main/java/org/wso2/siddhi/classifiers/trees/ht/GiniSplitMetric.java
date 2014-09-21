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

import java.util.List;
import java.util.Map;

public class GiniSplitMetric extends SplitMetric {

    /**
     * For serialization
     */
    private static final long serialVersionUID = -2037586582742660298L;

    @Override
    public double evaluateSplit(Map<String, WeightMass> preDist,
                                List<Map<String, WeightMass>> postDist) {
        double totalWeight = 0.0;
        double[] distWeights = new double[postDist.size()];

        for (int i = 0; i < postDist.size(); i++) {
            distWeights[i] = SplitMetric.sum(postDist.get(i));
            totalWeight += distWeights[i];
        }
        double gini = 0;
        for (int i = 0; i < postDist.size(); i++) {
            gini += (distWeights[i] / totalWeight)
                    * gini(postDist.get(i), distWeights[i]);
        }

        return 1.0 - gini;
    }

    /**
     * Return the gini metric computed from the supplied distribution
     *
     * @param dist the distribution to compute the gini metric from
     * @param sumOfWeights the sum of the distribution weights
     * @return the gini metric
     */
    protected static double gini(Map<String, WeightMass> dist, double sumOfWeights) {
        double gini = 1.0;

        for (Map.Entry<String, WeightMass> e : dist.entrySet()) {
            double frac = e.getValue().m_weight / sumOfWeights;
            gini -= frac * frac;
        }

        return gini;
    }

    /**
     * Return the gini metric computed from the supplied distribution
     *
     * @param dist dist the distribution to compute the gini metric from
     * @return
     */
    public static double gini(Map<String, WeightMass> dist) {
        return gini(dist, SplitMetric.sum(dist));
    }

    @Override
    public double getMetricRange(Map<String, WeightMass> preDist) {
        return 1.0;
    }

}
