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

import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;

public class Runtime{
    private final long WINDOW_SIZE = 1000l;

    /**
     * Calculate Window Coverage
     *
     * @param eventTimeStamps
     * @return
     */
    public double calculateWindowCoverage(LinkedHashSet<Long> eventTimeStamps) {
        double windowCoverage = -1;
        long count = 1;
        int numerator = 0;
        int denominator = 0;
        long lowerIndex = 0;
        long largestTimeStamp = -1;
        long d = -1;
        ArrayList timeStamps = new ArrayList();
        timeStamps.addAll(eventTimeStamps);
        Iterator<Long> itr;
        itr = eventTimeStamps.iterator();
        largestTimeStamp = getLargestTimeStamp(eventTimeStamps);

        if(itr.hasNext()) {
            d = (Long) itr.next();
            long edgeValue = (largestTimeStamp - WINDOW_SIZE - 1);
            long distance = Math.abs(d - edgeValue);

            while (itr.hasNext()) {
                long c = (Long) itr.next();
                long cdistance = Math.abs(c - edgeValue);

                if ((cdistance < distance) && (cdistance != 0)) {
                    distance = cdistance;
                    lowerIndex = count;
                }
                count += 1;
            }

            try {
                for (long i = edgeValue + 1; i <= largestTimeStamp - 1; i++) {
                    if (eventTimeStamps.contains(i)) {
                        int z = timeStamps.indexOf(i);
                        int y = timeStamps.indexOf(largestTimeStamp);

                        if ((z <= (y - 1)) && (z >= lowerIndex)) {
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

    private long getLargestTimeStamp(LinkedHashSet<Long> eventTimeStamps){
        Iterator<Long> itr = eventTimeStamps.iterator();
        long largestItem = 0;
        long item = 0;

        while(itr.hasNext()){
            item = itr.next();

            if(item > largestItem){
                largestItem = item;
            }
        }

        return largestItem;
    }
}
