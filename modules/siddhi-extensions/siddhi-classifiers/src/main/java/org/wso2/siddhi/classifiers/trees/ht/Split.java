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

import java.util.ArrayList;
import java.util.List;

public abstract class Split {
    /** name(s) of attribute(s) involved in the split */
    protected List<String> splitNames = new ArrayList<String>();

    /**
     * Returns the name of the branch that the supplied instance would go down
     *
     * @param inst the instance to find the branch for
     * @return the name of the branch that the instance would go down
     */
    public abstract String branchForInstance(Instance inst);

    /**
     * Returns the condition for the supplied branch name
     *
     * @param branch the name of the branch to get the condition for
     * @return the condition (test) that corresponds to the named branch
     */
    public abstract String conditionForBranch(String branch);

    public List<String> splitAttributes() {
        return splitNames;
    }
}
