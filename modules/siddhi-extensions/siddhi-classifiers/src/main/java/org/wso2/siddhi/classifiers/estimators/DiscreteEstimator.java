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

public class DiscreteEstimator extends Estimator {
    /** Hold the counts */
    private final double[] counts;

    /** Hold the sum of counts */
    private double sumOfCounts;

    /** Initialization for counts */
    private double priorCount;
    /**
     * Constructor
     *
     * @param numSymbols the number of possible symbols (remember to include 0)
     * @param laplace if true, counts will be initialised to 1
     */
    public DiscreteEstimator(int numSymbols, boolean laplace) {

        counts = new double[numSymbols];
        sumOfCounts = 0;
        if (laplace) {
            priorCount = 1;
            for (int i = 0; i < numSymbols; i++) {
                counts[i] = 1;
            }
            sumOfCounts = numSymbols;
        }
    }

    public void addValue(double data, double weight) {

        counts[(int) data] += weight;
        sumOfCounts += weight;
    }

    public double getProbability(double data) {

        if (sumOfCounts == 0) {
            return 0;
        }
        return counts[(int) data] / sumOfCounts;
    }

}
