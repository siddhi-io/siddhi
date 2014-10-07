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

public class UnivariateNormalEstimator {
    /** The weighted sum of values */
    protected double weightedSum = 0;

    /** The weighted sum of squared values */
    protected double weightedSumSquared = 0;

    /** The weight of the values collected so far */
    protected double sumOfWeights = 0;

    /** The mean value (only updated when needed) */
    protected double mean = 0;

    /** The variance (only updated when needed) */
    protected double variance = Double.MAX_VALUE;

    /** The minimum allowed value of the variance (default: 1.0E-6 * 1.0E-6) */
    protected double minVar = 1.0E-6 * 1.0E-6;

    /** Constant for Gaussian density */
    public static final double CONST = Math.log(2 * Math.PI);

    /**
     * Adds a value to the density estimator.
     *
     * @param value the value to add
     * @param weight the weight of the value
     */
    public void addValue(double value, double weight) {

        weightedSum += value * weight;
        weightedSumSquared += value * value * weight;
        sumOfWeights += weight;
    }

    /**
     * Updates mean and variance based on sufficient statistics.
     * Variance is set to minVar if it becomes smaller than that
     * value. It is set to Double.MAX_VALUE if the sum of weights is
     * zero.
     */
    protected void updateMeanAndVariance() {

        // Compute mean
        mean = 0;
        if (sumOfWeights > 0) {
            mean = weightedSum / sumOfWeights;
        }

        // Compute variance
        variance = Double.MAX_VALUE;
        if (sumOfWeights > 0) {
            variance = weightedSumSquared / sumOfWeights - mean * mean;
        }

        // Hack for case where variance is 0
        if (variance <= minVar) {
            variance = minVar;
        }
    }
}
