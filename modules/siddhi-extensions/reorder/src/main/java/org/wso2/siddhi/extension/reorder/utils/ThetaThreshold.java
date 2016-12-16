/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.extension.reorder.alphakslack;

import org.apache.commons.math3.distribution.NormalDistribution;

import java.util.ArrayList;

public class ThetaThreshold {
    private double delta;
    private double errorThreshold;
    private long N;
    private double idealMean;
    private double idealVariance;

    public ThetaThreshold(double errorThreshold, double delta) {
        this.errorThreshold = errorThreshold;
        this.delta = delta;
    }

    /**
     * Calculation of CriticalValue
     *
     * @return
     */
    public double calculateCriticalValue() {
        NormalDistribution actualDistribution = new NormalDistribution();
        double criticalValue = Math.abs(actualDistribution.inverseCumulativeProbability(delta / 2));

        return criticalValue;
    }

    public double calculateMean(ArrayList listOfEvents) {
        N = listOfEvents.size();
        long mean = 0;
        for (int i = 0; i < listOfEvents.size(); i++) {
            mean += (Long)listOfEvents.get(i);
        }
        idealMean = mean *1.0/ listOfEvents.size();
        return idealMean;
    }

    public double calculateVariance(ArrayList listOfEvents) {
        double variance = 0;
        for (int i = 0; i < listOfEvents.size(); i++) {
            variance += Math.pow(idealMean - (Long)listOfEvents.get(i), 2);
        }
        idealVariance = variance*1.0 / listOfEvents.size();
        return idealVariance;
    }

    /**
     * Solving inequality to get ThetaThreshold
     *
     * @param criticalValue
     * @param idealMean
     * @param idealVariance
     * @return
     */
    public double calculateThetaThreshold(double criticalValue, double idealMean, double idealVariance) {
        double temp1 = Math.sqrt((Math.pow(idealMean, 2) + Math.pow(idealVariance, 2)) / (N * Math.pow(idealMean, 2)));
        double temp2 = Math.pow(criticalValue, 2) * Math.pow(temp1, 2);
        double a1, b1, c1, b2, c2;
        double thetaThresholdValue;
        double theta1;
        double theta2;
        double theta3;
        double theta4;
        double tempSq1;
        double tempSq2;

        a1 = 1 + temp2;
        b1 = (2 * errorThreshold) - 2 - temp2;
        c1 = Math.pow((1 - errorThreshold), 2);
        b2 = -(2 + (2 * errorThreshold) + temp2);
        c2 = Math.pow((1 + errorThreshold), 2);
        tempSq1 = Math.pow(b1, 2) - (4 * a1 * c1);
        tempSq2 = Math.pow(b2, 2) - (4 * a1 * c2);

        if (tempSq1 >= 0) {
            theta1 = (-b1 - Math.sqrt(tempSq1)) / (2 * a1);
            theta2 = (-b1 + Math.sqrt(tempSq1)) / (2 * a1);
        } else {
            theta1 = theta2 = 0;
        }

        if (tempSq2 >= 0) {
            theta3 = (-b2 - Math.sqrt(tempSq2)) / (2 * a1);
            theta4 = (-b2 + Math.sqrt(tempSq2)) / (2 * a1);
        } else {
            theta3 = theta4 = 0;
        }


        if ((tempSq1 >= 0) && (tempSq2 < 0)) {
            thetaThresholdValue = Math.min(theta1, theta2);
        } else if ((tempSq2 >= 0) && (tempSq1 < 0)) {
            thetaThresholdValue = Math.min(theta3, theta4);
        } else if ((tempSq1 < 0) && (tempSq2 < 0)) {
            thetaThresholdValue = 0;
        } else {
            thetaThresholdValue = Math.min(Math.min(theta1, theta2), Math.min(theta3, theta4));
        }
        return thetaThresholdValue;
    }
}