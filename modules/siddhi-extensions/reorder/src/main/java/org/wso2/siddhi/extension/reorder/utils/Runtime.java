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

import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class Runtime {
    /**
     * Calculate Window Coverage
     *
     * @param timestampList
     * @param windowSize
     * @return
     */
    public double calculateWindowCoverage(List<Long> timestampList, long windowSize) {
        double windowCoverage = -1;
        int numerator = 0;
        int denominator = 0;
        long lowerIndex = 0;
        long largestTimestamp = -1;
        long timestamp = -1;
        Iterator<Long> timestampEntry = timestampList.iterator();
        largestTimestamp = Collections.max(timestampList);
        int indexOfLargestTimestamp = timestampList.indexOf(largestTimestamp);
        if (timestampEntry.hasNext()) {
            timestamp = timestampEntry.next();
            long edgeValue = (largestTimestamp - windowSize);
            long distance = Math.abs(timestamp - edgeValue);
            while (timestampEntry.hasNext()) {
                timestamp = timestampEntry.next();
                long cdistance = Math.abs(timestamp - edgeValue);
                if (cdistance < distance) {
                    distance = cdistance;
                    lowerIndex = timestampList.indexOf(timestamp);
                }
            }
            try {
                for (long i = edgeValue; i <= largestTimestamp; i++) {
                    if (timestampList.contains(i)) {
                        int z = timestampList.indexOf(i);
                        if ((z <= indexOfLargestTimestamp) && (z >= lowerIndex)) {
                            numerator += 1;
                        }
                        denominator += 1;
                    }
                }
                windowCoverage = numerator * 1.0 / denominator;
            } catch (Exception e) {
                throw new ExecutionPlanRuntimeException("Error in Window Coverage Calculation.", e);
            }
        }
        return windowCoverage;
    }
}
