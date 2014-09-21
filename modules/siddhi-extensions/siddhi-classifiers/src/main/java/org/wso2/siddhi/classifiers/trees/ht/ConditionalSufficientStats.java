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

import java.util.HashMap;
import java.util.Map;

public abstract class ConditionalSufficientStats {

    /**
     * For serialization
     */
    private static final long serialVersionUID = 8724787722646808376L;

    /** Lookup by class value */
    protected Map<String, Object> m_classLookup = new HashMap<String, Object>();

    /**
     * Update this stat with the supplied attribute value and class value
     *
     * @param attVal the value of the attribute
     * @param classVal the class value
     * @param weight the weight of this observation
     */
    public abstract void update(double attVal, String classVal, double weight);

    /**
     * Return the probability of an attribute value conditioned on a class value
     *
     * @param attVal the attribute value to compute the conditional probability
     *          for
     * @param classVal the class value
     * @return the probability
     */
    public abstract double probabilityOfAttValConditionedOnClass(double attVal,
                                                                 String classVal);

    /**
     * Return the best split
     *
     * @param splitMetric the split metric to use
     * @param preSplitDist the distribution of class values prior to splitting
     * @param attName the name of the attribute being considered for splitting
     * @return the best split for the attribute
     */
    public abstract SplitCandidate bestSplit(SplitMetric splitMetric,
                                             Map<String, WeightMass> preSplitDist, String attName);
}
