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

import org.wso2.siddhi.classifiers.trees.ht.utils.Utils;

public class ContingencyTables {
    public static final double log2 = Math.log(2);

    /** Cache of integer logs */
    private static final double MAX_INT_FOR_CACHE_PLUS_ONE = 10000;
    private static final double[] INT_N_LOG_N_CACHE = new double[(int)MAX_INT_FOR_CACHE_PLUS_ONE];

    /** Initialize cache */
    static {
        for (int i = 1; i < MAX_INT_FOR_CACHE_PLUS_ONE; i++) {
            double d = (double)i;
            INT_N_LOG_N_CACHE[i] = d * Math.log(d);
        }
    }
    /**
     * Computes the entropy of the given array.
     *
     * @param array the array
     * @return the entropy
     */
    public static double entropy(double[] array) {

        double returnValue = 0, sum = 0;

        for (int i = 0; i < array.length; i++) {
            returnValue -= lnFunc(array[i]);
            sum += array[i];
        }
        if (Utils.eq(sum, 0)) {
            return 0;
        } else {
            return (returnValue + lnFunc(sum)) / (sum * log2);
        }
    }

    /**
     * Help method for computing entropy.
     */
    public static double lnFunc(double num){

        if (num <= 0) {
            return 0;
        } else {

            // Use cache if we have a sufficiently small integer
            if (num < MAX_INT_FOR_CACHE_PLUS_ONE) {
                int n = (int)num;
                if ((double)n == num) {
                    return INT_N_LOG_N_CACHE[n];
                }
            }
            return num * Math.log(num);
        }
    }
}
