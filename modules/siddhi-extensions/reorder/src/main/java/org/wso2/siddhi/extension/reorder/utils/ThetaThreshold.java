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

package org.wso2.siddhi.extension.reorder.utils;

import org.apache.commons.math3.distribution.NormalDistribution;

import java.util.Iterator;
import java.util.List;

public class ThetaThreshold {
    private double delta;
    private double errorThreshold;
    private long listSize;
    private double mean;
    private double variance;

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

    public double calculateMean(List<Double> listOfEvents) {
        listSize = listOfEvents.size();
        double sum = 0;
        Iterator<Double> entries = listOfEvents.iterator();
        while (entries.hasNext()) {
            sum += entries.next();
        }
        mean = sum * 1.0 / listSize;
        return mean;
    }

    public double calculateVariance(List<Double> listOfEvents) {
        double squaredSum = 0;
        double temp=0;
        Iterator<Double> entries = listOfEvents.iterator();
        while (entries.hasNext()) {
            temp = mean - entries.next();
            squaredSum += Math.pow(temp, 2);
        }
        variance = squaredSum * 1.0 / (listSize);
        return variance;
    }

    /**
     * Solving inequality to get ThetaThreshold
     *
     * @param criticalValue
     * @param mean
     * @param variance
     * @return
     */
    public double calculateThetaThreshold(double criticalValue, double mean, double variance) {
        double temp1 = Math.sqrt((Math.pow(mean, 2) + Math.pow(variance, 2)) / (listSize * Math.pow(mean, 2)));
        double temp2 = Math.pow(criticalValue, 2) * Math.pow(temp1, 2);
        double a1, b1, c1, b2, c2;
        double thetaThresholdValue;
        double theta1,theta2,theta3,theta4,tempSq1,tempSq2;

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