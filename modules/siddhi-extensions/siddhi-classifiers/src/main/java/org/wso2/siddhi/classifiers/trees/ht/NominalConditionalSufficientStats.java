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

import java.io.Serializable;
import java.util.*;

public class NominalConditionalSufficientStats extends ConditionalSufficientStats {

    /**
     * For serialization
     */
    private static final long serialVersionUID = -669902060601313488L;

    /**
     * Inner class that implements a discrete distribution
     *
     * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
     */
    protected class ValueDistribution implements Serializable {

        /**
         * For serialization
         */
        private static final long serialVersionUID = -61711544350888154L;

        protected final Map<Integer, WeightMass> m_dist = new LinkedHashMap<Integer, WeightMass>();

        private double m_sum;

        public void add(int val, double weight) {
            WeightMass count = m_dist.get(val);
            if (count == null) {
                count = new WeightMass();
                count.m_weight = 1.0;
                m_sum += 1.0;
                m_dist.put(val, count);
            }
            count.m_weight += weight;
            m_sum += weight;
        }

        public void delete(int val, double weight) {
            WeightMass count = m_dist.get(val);
            if (count != null) {
                count.m_weight -= weight;
                m_sum -= weight;
            }
        }

        public double getWeight(int val) {
            WeightMass count = m_dist.get(val);
            if (count != null) {
                return count.m_weight;
            }

            return 0.0;
        }

        public double sum() {
            return m_sum;
        }
    }

    protected double m_totalWeight;
    protected double m_missingWeight;

    @Override
    public void update(double attVal, String classVal, double weight) {
        if (Utils.isMissingValue(attVal)) {
            m_missingWeight += weight;
        } else {
            new Integer((int) attVal);
            ValueDistribution valDist = (ValueDistribution) m_classLookup
                    .get(classVal);
            if (valDist == null) {
                valDist = new ValueDistribution();
                valDist.add((int) attVal, weight);
                m_classLookup.put(classVal, valDist);
            } else {
                valDist.add((int) attVal, weight);
            }
        }

        m_totalWeight += weight;
    }

    @Override
    public double probabilityOfAttValConditionedOnClass(double attVal,
                                                        String classVal) {
        ValueDistribution valDist = (ValueDistribution) m_classLookup.get(classVal);
        if (valDist != null) {
            double prob = valDist.getWeight((int) attVal) / valDist.sum();
            return prob;
        }

        return 0;
    }

    protected List<Map<String, WeightMass>> classDistsAfterSplit() {

        // att index keys to class distribution
        Map<Integer, Map<String, WeightMass>> splitDists = new HashMap<Integer, Map<String, WeightMass>>();

        for (Map.Entry<String, Object> cls : m_classLookup.entrySet()) {
            String classVal = cls.getKey();
            ValueDistribution attDist = (ValueDistribution) cls.getValue();

            for (Map.Entry<Integer, WeightMass> att : attDist.m_dist.entrySet()) {
                Integer attVal = att.getKey();
                WeightMass attCount = att.getValue();

                Map<String, WeightMass> clsDist = splitDists.get(attVal);
                if (clsDist == null) {
                    clsDist = new HashMap<String, WeightMass>();
                    splitDists.put(attVal, clsDist);
                }

                WeightMass clsCount = clsDist.get(classVal);

                if (clsCount == null) {
                    clsCount = new WeightMass();
                    clsDist.put(classVal, clsCount);
                }

                clsCount.m_weight += attCount.m_weight;
            }

        }

        List<Map<String, WeightMass>> result = new LinkedList<Map<String, WeightMass>>();
        for (Map.Entry<Integer, Map<String, WeightMass>> v : splitDists.entrySet()) {
            result.add(v.getValue());
        }

        return result;
    }

    @Override
    public SplitCandidate bestSplit(SplitMetric splitMetric,
                                    Map<String, WeightMass> preSplitDist, String attName) {

        List<Map<String, WeightMass>> postSplitDists = classDistsAfterSplit();
        double merit = splitMetric.evaluateSplit(preSplitDist, postSplitDists);
        SplitCandidate candidate = new SplitCandidate(
                new UnivariateNominalMultiwaySplit(attName), postSplitDists, merit);

        return candidate;
    }

}
