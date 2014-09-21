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
package org.wso2.siddhi.classifiers;

import org.wso2.siddhi.classifiers.trees.ht.Instance;
import org.wso2.siddhi.classifiers.trees.ht.utils.Utils;
import org.wso2.siddhi.query.api.definition.Attribute;

public abstract class AbstractClassifier implements Classifier {
    /**
     * Classifies the given test instance. The instance has to belong to a dataset
     * when it's being classified. Note that a classifier MUST implement either
     * this or distributionForInstance().
     *
     * @param instance the instance to be classified
     * @return the predicted most likely class for the instance or
     *         Utils.missingValue() if no prediction is made
     * @exception Exception if an error occurred during the prediction
     */
    public double classifyInstance(Instance instance) throws Exception {

        double[] dist = distributionForInstance(instance);
        if (dist == null) {
            throw new Exception("Null distribution predicted");
        }
        switch (instance.classAttribute().getType()) {
            case NOMINAL:
                double max = 0;
                int maxIndex = 0;

                for (int i = 0; i < dist.length; i++) {
                    if (dist[i] > max) {
                        maxIndex = i;
                        max = dist[i];
                    }
                }
                if (max > 0) {
                    return maxIndex;
                } else {
                    return Utils.missingValue();
                }
            case NUMERIC:
            case DATE:
                return dist[0];
            default:
                return Utils.missingValue();
        }
    }

    /**
     * Predicts the class memberships for a given instance. If an instance is
     * unclassified, the returned array elements must be all zero. If the class is
     * numeric, the array must consist of only one element, which contains the
     * predicted value. Note that a classifier MUST implement either this or
     * classifyInstance().
     *
     * @param instance the instance to be classified
     * @return an array containing the estimated membership probabilities of the
     *         test instance in each class or the numeric prediction
     * @exception Exception if distribution could not be computed successfully
     */
    public double[] distributionForInstance(Instance instance) throws Exception {

        double[] dist = new double[instance.numClasses()];
        switch (instance.classAttribute().getType()) {
            case NOMINAL:
                double classification = classifyInstance(instance);
                if (Utils.isMissingValue(classification)) {
                    return dist;
                } else {
                    dist[(int) classification] = 1.0;
                }
                return dist;
            case NUMERIC:
            case DATE:
                dist[0] = classifyInstance(instance);
                return dist;
            default:
                return dist;
        }
    }
}
