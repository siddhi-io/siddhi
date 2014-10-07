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
package org.wso2.siddhi.classifiers.estimators;

import org.wso2.siddhi.classifiers.trees.ht.Instances;

public abstract class Estimator {
    public void addValue(double data, double weight) {
        try {
            throw new Exception("Method to add single value is not implemented!\n"
                    + "Estimator should implement IncrementalEstimator.");
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println(ex.getMessage());
        }
    }


    /**
     * Initialize the estimator with all values of one attribute of a dataset.
     * Some estimator might ignore the min and max values.
     *
     * @param data the dataset used to build this estimator
     * @param attrIndex attribute the estimator is for
     * @param min minimal border of range
     * @param max maximal border of range
     * @param factor number of instances has been reduced to that factor
     * @exception Exception if building of estimator goes wrong
     */
    public void addValues(Instances data, int attrIndex, double min, double max,
                          double factor) throws Exception {
        // no handling of factor, would have to be overridden

        // no handling of min and max, would have to be overridden

        int numInst = data.numInstances();
        for (int i = 1; i < numInst; i++) {
            addValue(data.instance(i).value(attrIndex), 1.0);
        }
    }


    public abstract double getProbability(double data);
}
