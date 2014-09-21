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

import org.wso2.siddhi.query.api.definition.Attribute;

public class UnivariateNumericBinarySplit extends Split {
    /** The split point */
    protected double m_splitPoint;

    /**
     * Constructor
     *
     * @param attName the name of the attribute to split on
     * @param splitPoint the split point
     */
    public UnivariateNumericBinarySplit(String attName, double splitPoint) {
        m_splitAttNames.add(attName);
        m_splitPoint = splitPoint;
    }

    @Override
    public String branchForInstance(Instance inst) {

        Attribute att = inst.dataset().attribute(m_splitAttNames.get(0));
        if (att == null || inst.isMissing(att)) {
            // TODO -------------
            return null;
        }

        if (inst.value(att) <= m_splitPoint) {
            return "left";
        }

        return "right";
    }

    @Override
    public String conditionForBranch(String branch) {
        String result = m_splitAttNames.get(0);

        if (branch.equalsIgnoreCase("left")) {
            result += " <= ";
        } else {
            result += " > ";
        }

        result += String.format("%-9.3f", m_splitPoint);

        return result;
    }
}
