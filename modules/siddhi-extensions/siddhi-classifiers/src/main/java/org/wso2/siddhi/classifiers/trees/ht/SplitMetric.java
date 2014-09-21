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

public abstract class SplitMetric {
    /**
     * For serialization
     */
    private static final long serialVersionUID = 2891555018707080818L;

    /**
     * Utility method to return the sum of instance weight in a distribution
     *
     * @param dist the distribution
     * @return the sum of the weights contained in a distribution
     */
    public static double sum(Map<String, WeightMass> dist) {
        double sum = 0;

        for (Map.Entry<String, WeightMass> e : dist.entrySet()) {
            sum += e.getValue().m_weight;
        }

        return sum;
    }

    /**
     * Evaluate the merit of a split
     *
     * @param preDist the class distribution before the split
     * @param postDist the class distributions after the split
     * @return the merit of the split
     */
    public abstract double evaluateSplit(Map<String, WeightMass> preDist,
                                         List<Map<String, WeightMass>> postDist);

    /**
     * Get the range of the splitting metric
     *
     * @param preDist the pre-split class distribution
     * @return the range of the splitting metric
     */
    public abstract double getMetricRange(Map<String, WeightMass> preDist);
}
