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

public class SplitCandidate implements Comparable<SplitCandidate> {

    public Split splitTest;

    /**
     * list of class distributions resulting from a split - 2 entries in the outer
     * list for numeric splits and n for nominal splits
     */
    public List<Map<String, WeightMass>> postSplitClassDistributions;

    /** The merit of the split */
    public double splitMerit;

    /**
     * Constructor
     *
     * @param splitTest the splitting test
     * @param postSplitDists the distributions resulting from the split
     * @param merit the merit of the split
     */
    public SplitCandidate(Split splitTest,
                          List<Map<String, WeightMass>> postSplitDists, double merit) {
        this.splitTest = splitTest;
        postSplitClassDistributions = postSplitDists;
        splitMerit = merit;
    }

    /**
     * Number of branches resulting from the split
     *
     * @return the number of subsets of instances resulting from the split
     */
    public int numSplits() {
        return postSplitClassDistributions.size();
    }

    public int compareTo(SplitCandidate comp) {
        return Double.compare(splitMerit, comp.splitMerit);
    }
}
