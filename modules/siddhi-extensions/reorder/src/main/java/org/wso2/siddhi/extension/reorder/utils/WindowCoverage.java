package org.wso2.siddhi.extension.reorder.utils;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class WindowCoverage {
    private double errorThreshold;

    public WindowCoverage(double errorThreshold){
        this.errorThreshold = errorThreshold;
    }

    /**
     * The following method - calculateWindowCoverageThreshold returns window coverage threshold
     * which is found as a minimum value that satisfying two inequalities in the form of :
     *              a1(x^2) + b1(x) + c1 <= 0    and
     *              a1(x^2) + b2(x) + c2 <=0
     */

    /**
     * Calculate Window Coverage Threshold
     *
     * @param criticalValue
     * @param listOfData
     * @return windowCoverageThreshold
     */

    public double calculateWindowCoverageThreshold(double criticalValue, List<Double> listOfData) {
        double mean = calculateMean(listOfData);
        double variance = calculateVariance(listOfData);
        double temp1 = Math.sqrt((Math.pow(mean, 2) + Math.pow(variance, 2)) /
                (listOfData.size() * Math.pow(mean, 2)));
        double temp2 = Math.pow(criticalValue, 2) * Math.pow(temp1, 2);
        double a1, b1, c1, b2, c2;
        double windowCoverageThreshold;
        double theta1, theta2, theta3, theta4, tempSq1, tempSq2;
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
            windowCoverageThreshold = Math.min(theta1, theta2);
        } else if ((tempSq2 >= 0) && (tempSq1 < 0)) {
            windowCoverageThreshold = Math.min(theta3, theta4);
        } else if ((tempSq1 < 0) && (tempSq2 < 0)) {
            windowCoverageThreshold = 0;
        } else {
            windowCoverageThreshold = Math.min(Math.min(theta1, theta2), Math.min(theta3, theta4));
        }
        return windowCoverageThreshold;
    }

    private double calculateMean(List<Double> listOfData){
        double sum = 0;
        for (Double aListOfData : listOfData) {
            sum += aListOfData;
        }
        return sum / listOfData.size();
    }

    private  double calculateVariance(List<Double> listOfData){
        double squaredSum = 0;
        for (Double aListOfData : listOfData) {
            squaredSum += Math.pow((calculateMean(listOfData) - aListOfData), 2);
        }
        return squaredSum / listOfData.size();
    }

    /**
     * Calculate Window Coverage
     *
     * @param eventTimestamps
     * @param windowSize
     * @return runtimeWindowCoverage
     */

    public double calculateRuntimeWindowCoverage(List<Long> eventTimestamps, long windowSize){
        double runtimeWindowCoverage = -1;
        int numerator = 0;
        int denominator = 0;
        long lowerIndex = 0;
        long timestamp;
        Iterator<Long> timestampEntryIterator = eventTimestamps.iterator();
        long largestTimestamp = Collections.max(eventTimestamps);
        int indexOfLargestTimestamp = eventTimestamps.indexOf(largestTimestamp);
        long edgeValue = (largestTimestamp - windowSize);
        if (timestampEntryIterator.hasNext()) {
            timestamp = timestampEntryIterator.next();
            long distance = Math.abs(timestamp - edgeValue);
            while (timestampEntryIterator.hasNext()) {
                timestamp = timestampEntryIterator.next();
                long cdistance = Math.abs(timestamp - edgeValue);
                if (cdistance < distance) {
                    distance = cdistance;
                    lowerIndex = eventTimestamps.indexOf(timestamp);
                }
            }
            for (long i = edgeValue; i <= largestTimestamp; i++) {
                int z = eventTimestamps.indexOf(i);
                if(z >= 0) {
                    if ((z <= indexOfLargestTimestamp) && (z >= lowerIndex)) {
                        numerator += 1;
                    }
                    denominator += 1;
                }
            }
            runtimeWindowCoverage = numerator * 1.0 / denominator;
        }
        return runtimeWindowCoverage;
    }
}
