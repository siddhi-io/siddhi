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

import org.wso2.siddhi.classifiers.trees.ht.nodes.LeafNode;
import org.wso2.siddhi.classifiers.trees.ht.nodes.LearningNode;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ActiveHNode extends LeafNode implements LearningNode {
    /** The weight of instances seen at the last split evaluation */
    public double weightSeenAtLastSplitEval = 0;

    /** Statistics for nominal or numeric attributes conditioned on the class */
    protected Map<String, ConditionalSufficientStats> m_nodeStats = new HashMap<String, ConditionalSufficientStats>();

    @Override
    public void updateNode(Instance inst) throws Exception {
        super.updateDistribution(inst);

        for (int i = 0; i < inst.numAttributes(); i++) {
            Attribute a = inst.attribute(i);
            if (i != inst.classIndex()) {
                ConditionalSufficientStats stats = m_nodeStats.get(a.getName());
                if (stats == null) {
                    if (a.isNumeric()) {
                        stats = new GaussianConditionalSufficientStats();
                    } else {
                        stats = new NominalConditionalSufficientStats();
                    }
                    m_nodeStats.put(a.getName(), stats);
                }

                stats.update(inst.value(a),
                        inst.classAttribute().value((int) inst.classValue()),
                        inst.weight());
            }
        }
    }

    /**
     * Returns a list of split candidates
     *
     * @param splitMetric the splitting metric to use
     * @return a list of split candidates
     */
    public List<SplitCandidate> getPossibleSplits(SplitMetric splitMetric) {

        List<SplitCandidate> splits = new ArrayList<SplitCandidate>();

        // null split
        List<Map<String, WeightMass>> nullDist = new ArrayList<Map<String, WeightMass>>();
        nullDist.add(weightedClassDist);
        SplitCandidate nullSplit = new SplitCandidate(null, nullDist,
                splitMetric.evaluateSplit(weightedClassDist, nullDist));
        splits.add(nullSplit);

        for (Map.Entry<String, ConditionalSufficientStats> e : m_nodeStats
                .entrySet()) {
            ConditionalSufficientStats stat = e.getValue();

            SplitCandidate splitCandidate = stat.bestSplit(splitMetric,
                    weightedClassDist, e.getKey());

            if (splitCandidate != null) {
                splits.add(splitCandidate);
            }
        }

        return splits;
    }
}
