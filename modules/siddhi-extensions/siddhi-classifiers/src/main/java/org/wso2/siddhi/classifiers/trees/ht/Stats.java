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

import java.io.Serializable;

public class Stats implements Serializable {
    private static final long serialVersionUID = 4585789825245385836L;

    /** for serialization */
    /**
     * The number of values seen
     */
    public double count = 0;

    /**
     * The sum of values seen
     */
    public double sum = 0;

    /**
     * The sum of values squared seen
     */
    public double sumSq = 0;

    /**
     * The std deviation of values at the last calculateDerived() call
     */
    public double stdDev = Double.NaN;

    /**
     * The mean of values at the last calculateDerived() call
     */
    public double mean = Double.NaN;

    /**
     * The minimum value seen, or Double.NaN if no values seen
     */
    public double min = Double.NaN;

    /**
     * The maximum value seen, or Double.NaN if no values seen
     */
    public double max = Double.NaN;

    /**
     * Adds a value to the observed values
     *
     * @param value the observed value
     */
    public void add(double value) {

        add(value, 1);
    }

    /**
     * Adds a value that has been seen n times to the observed values
     *
     * @param value the observed value
     * @param n     the number of times to add value
     */
    public void add(double value, double n) {

        sum += value * n;
        sumSq += value * value * n;
        count += n;
        if (Double.isNaN(min)) {
            min = max = value;
        } else if (value < min) {
            min = value;
        } else if (value > max) {
            max = value;
        }
    }

    /**
     * Removes a value to the observed values (no checking is done
     * that the value being removed was actually added).
     *
     * @param value the observed value
     */
    public void subtract(double value) {
        subtract(value, 1);
    }

    /**
     * Subtracts a value that has been seen n times from the observed values
     *
     * @param value the observed value
     * @param n     the number of times to subtract value
     */
    public void subtract(double value, double n) {
        sum -= value * n;
        sumSq -= value * value * n;
        count -= n;
    }

    /**
     * Tells the object to calculate any statistics that don't have their
     * values automatically updated during add. Currently updates the mean
     * and standard deviation.
     */
    public void calculateDerived() {

        mean = Double.NaN;
        stdDev = Double.NaN;
        if (count > 0) {
            mean = sum / count;
            stdDev = Double.POSITIVE_INFINITY;
            if (count > 1) {
                stdDev = sumSq - (sum * sum) / count;
                stdDev /= (count - 1);
                if (stdDev < 0) {
                    //          System.err.println("Warning: stdDev value = " + stdDev
                    //                             + " -- rounded to zero.");
                    stdDev = 0;
                }
                stdDev = Math.sqrt(stdDev);
            }
        }
    }

    /**
     * Returns a string summarising the stats so far.
     *
     * @return the summary string
     */
    public String toString() {

        calculateDerived();
        return
                "Count   " + Utils.doubleToString(count, 8) + '\n'
                        + "Min     " + Utils.doubleToString(min, 8) + '\n'
                        + "Max     " + Utils.doubleToString(max, 8) + '\n'
                        + "Sum     " + Utils.doubleToString(sum, 8) + '\n'
                        + "SumSq   " + Utils.doubleToString(sumSq, 8) + '\n'
                        + "Mean    " + Utils.doubleToString(mean, 8) + '\n'
                        + "StdDev  " + Utils.doubleToString(stdDev, 8) + '\n';
    }

    /**
     * Tests the paired stats object from the command line.
     * reads line from stdin, expecting two values per line.
     *
     * @param args ignored.
     */
    public static void main(String[] args) {

        try {
            Stats ps = new Stats();
            java.io.LineNumberReader r = new java.io.LineNumberReader(
                    new java.io.InputStreamReader(System.in));
            String line;
            while ((line = r.readLine()) != null) {
                line = line.trim();
                if (line.equals("") || line.startsWith("@") || line.startsWith("%")) {
                    continue;
                }
                java.util.StringTokenizer s
                        = new java.util.StringTokenizer(line, " ,\t\n\r\f");
                int count = 0;
                double v1 = 0;
                while (s.hasMoreTokens()) {
                    double val = (new Double(s.nextToken())).doubleValue();
                    if (count == 0) {
                        v1 = val;
                    } else {
                        System.err.println("MSG: Too many values in line \""
                                + line + "\", skipped.");
                        break;
                    }
                    count++;
                }
                if (count == 1) {
                    ps.add(v1);
                }
            }
            ps.calculateDerived();
            System.err.println(ps);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.err.println(ex.getMessage());
        }
    }
}
