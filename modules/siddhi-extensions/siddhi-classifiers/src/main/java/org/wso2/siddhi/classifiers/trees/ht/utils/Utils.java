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
package org.wso2.siddhi.classifiers.trees.ht.utils;

public class Utils {
    public static double SMALL = 1e-6;

    public static double log2 = Math.log(2);

    /**
     * Normalizes the doubles in the array by their sum.
     *
     * @param doubles the array of double
     * @throws IllegalArgumentException if sum is Zero or NaN
     */
    public static void normalize(double[] doubles) {

        double sum = 0;
        for (double d : doubles) {
            sum += d;
        }
        normalize(doubles, sum);
    }

    /**
     * Normalizes the doubles in the array using the given value.
     *
     * @param doubles the array of double
     * @param sum     the value by which the doubles are to be normalized
     * @throws IllegalArgumentException if sum is zero or NaN
     */
    public static void normalize(double[] doubles, double sum) {

        if (Double.isNaN(sum)) {
            throw new IllegalArgumentException("Can't normalize array. Sum is NaN.");
        }
        if (sum == 0) {
            // Maybe this should just be a return.
            throw new IllegalArgumentException("Can't normalize array. Sum is zero.");
        }
        for (int i = 0; i < doubles.length; i++) {
            doubles[i] /= sum;
        }
    }

    public static boolean eq(double a, double b) {

        return (a == b) || ((a - b < SMALL) && (b - a < SMALL));
    }

    /**
     * Tests if a is smaller than b.
     *
     * @param a a double
     * @param b a double
     */
    public static boolean sm(double a, double b) {

        return (b - a > SMALL);
    }


    /**
     * Tests if a is greater than b.
     *
     * @param a a double
     * @param b a double
     */
    public static boolean gr(double a, double b) {

        return (a - b > SMALL);
    }

    public static int[] sort(double[] array) {

        int[] index = initialIndex(array.length);
        if (array.length > 1) {
            array = array.clone();
            replaceMissingWithMAX_VALUE(array);
            quickSort(array, index, 0, array.length - 1);
        }
        return index;
    }

    /**
     * Initial index, filled with values from 0 to size - 1.
     */
    private static int[] initialIndex(int size) {

        int[] index = new int[size];
        for (int i = 0; i < size; i++) {
            index[i] = i;
        }
        return index;
    }

    /**
     * Replaces all "missing values" in the given array of double values with
     * MAX_VALUE.
     *
     * @param array the array to be modified.
     */
    public static void replaceMissingWithMAX_VALUE(double[] array) {

        for (int i = 0; i < array.length; i++) {
            if (isMissingValue(array[i])) {
                array[i] = Double.MAX_VALUE;
            }
        }
    }

    /**
     * Tests if the given value codes "missing".
     *
     * @param val the value to be tested
     * @return true if val codes "missing"
     */
    public static boolean isMissingValue(double val) {

        return Double.isNaN(val);
    }

    private static void quickSort(int[] array,
                                  int[] index, int left, int right) {

        if (left < right) {
            int middle = partition(array, index, left, right);
            quickSort(array, index, left, middle);
            quickSort(array, index, middle + 1, right);
        }
    }

    private static void quickSort(double[] array,
                                  int[] index, int left, int right) {

        int diff = right - left;

        switch (diff) {
            case 0:

                // No need to do anything
                return;
            case 1:

                // Swap two elements if necessary
                conditionalSwap(array, index, left, right);
                return;
            case 2:

                // Just need to sort three elements
                conditionalSwap(array, index, left, left + 1);
                conditionalSwap(array, index, left, right);
                conditionalSwap(array, index, left + 1, right);
                return;
            default:

                // Establish pivot
                int pivotLocation = sortLeftRightAndCenter(array, index, left, right);

                // Move pivot to the right, partition, and restore pivot
                swap(index, pivotLocation, right - 1);
                int center = partition(array, index, left, right, array[index[right - 1]]);
                swap(index, center, right - 1);

                // Sort recursively
                quickSort(array, index, left, center - 1);
                quickSort(array, index, center + 1, right);
        }
    }

    /**
     * Swaps two elements in the given integer array.
     */
    private static void swap(int[] index, int l, int r) {

        int help = index[l];
        index[l] = index[r];
        index[r] = help;
    }

    /**
     * Partitions the instances around a pivot. Used by quicksort and
     * kthSmallestValue.
     *
     * @param array the array of doubles to be sorted
     * @param index the index into the array of doubles
     * @param l     the first index of the subset
     * @param r     the last index of the subset
     * @return the index of the middle element
     */
    private static int partition(double[] array, int[] index, int l, int r,
                                 double pivot) {

        r--;
        while (true) {
            while ((array[index[++l]] < pivot)) {
                ;
            }
            while ((array[index[--r]] > pivot)) {
                ;
            }
            if (l >= r) {
                return l;
            }
            swap(index, l, r);
        }
    }

    /**
     * Partitions the instances around a pivot. Used by quicksort and
     * kthSmallestValue.
     *
     * @param array the array of integers to be sorted
     * @param index the index into the array of integers
     * @param l     the first index of the subset
     * @param r     the last index of the subset
     * @return the index of the middle element
     */
    private static int partition(int[] array, int[] index, int l, int r) {

        double pivot = array[index[(l + r) / 2]];
        int help;

        while (l < r) {
            while ((array[index[l]] < pivot) && (l < r)) {
                l++;
            }
            while ((array[index[r]] > pivot) && (l < r)) {
                r--;
            }
            if (l < r) {
                help = index[l];
                index[l] = index[r];
                index[r] = help;
                l++;
                r--;
            }
        }
        if ((l == r) && (array[index[r]] > pivot)) {
            r--;
        }

        return r;
    }

    /**
     * Conditional swap for quick sort.
     */
    private static void conditionalSwap(double[] array, int[] index, int left,
                                        int right) {

        if (array[index[left]] > array[index[right]]) {
            int help = index[left];
            index[left] = index[right];
            index[right] = help;
        }
    }

    /**
     * Sorts left, right, and center elements only, returns resulting center as
     * pivot.
     */
    private static int sortLeftRightAndCenter(double[] array, int[] index, int l,
                                              int r) {

        int c = (l + r) / 2;
        conditionalSwap(array, index, l, c);
        conditionalSwap(array, index, l, r);
        conditionalSwap(array, index, c, r);
        return c;
    }

    /**
     * Casting an object without "unchecked" compile-time warnings. Use only when
     * absolutely necessary (e.g. when using clone()).
     */
    @SuppressWarnings("unchecked")
    public static <T> T cast(Object x) {
        return (T) x;
    }

    public static int[] sortWithNoMissingValues(
  double[] array) {

        int[] index = initialIndex(array.length);
        if (array.length > 1) {
            quickSort(array, index, 0, array.length - 1);
        }
        return index;
    }

    /**
     * Quotes a string if it contains special characters.
     *
     * The following rules are applied:
     *
     * A character is backquoted version of it is one of <tt>" ' % \ \n \r \t</tt>
     * .
     *
     * A string is enclosed within single quotes if a character has been
     * backquoted using the previous rule above or contains <tt>{ }</tt> or is
     * exactly equal to the strings <tt>, ? space or ""</tt> (empty string).
     *
     * A quoted question mark distinguishes it from the missing value which is
     * represented as an unquoted question mark in arff files.
     *
     * @param string the string to be quoted
     * @return the string (possibly quoted)
     */
    public static String quote(String string) {
        boolean quote = false;

        // backquote the following characters
        if ((string.indexOf('\n') != -1) || (string.indexOf('\r') != -1)
                || (string.indexOf('\'') != -1) || (string.indexOf('"') != -1)
                || (string.indexOf('\\') != -1) || (string.indexOf('\t') != -1)
                || (string.indexOf('%') != -1) || (string.indexOf('\u001E') != -1)) {
            string = backQuoteChars(string);
            quote = true;
        }

        // Enclose the string in 's if the string contains a recently added
        // backquote or contains one of the following characters.
        if ((quote == true) || (string.indexOf('{') != -1)
                || (string.indexOf('}') != -1) || (string.indexOf(',') != -1)
                || (string.equals("?")) || (string.indexOf(' ') != -1)
                || (string.equals(""))) {
            string = ("'".concat(string)).concat("'");
        }

        return string;
    }

    /**
     * Converts carriage returns and new lines in a string into \r and \n.
     * Backquotes the following characters: ` " \ \t and %
     *
     * @param string the string
     * @return the converted string
     */
    public static String backQuoteChars(String string) {

        int index;
        StringBuffer newStringBuffer;

        // replace each of the following characters with the backquoted version
        char charsFind[] = { '\\', '\'', '\t', '\n', '\r', '"', '%', '\u001E' };
        String charsReplace[] = { "\\\\", "\\'", "\\t", "\\n", "\\r", "\\\"",
                "\\%", "\\u001E" };
        for (int i = 0; i < charsFind.length; i++) {
            if (string.indexOf(charsFind[i]) != -1) {
                newStringBuffer = new StringBuffer();
                while ((index = string.indexOf(charsFind[i])) != -1) {
                    if (index > 0) {
                        newStringBuffer.append(string.substring(0, index));
                    }
                    newStringBuffer.append(charsReplace[i]);
                    if ((index + 1) < string.length()) {
                        string = string.substring(index + 1);
                    } else {
                        string = "";
                    }
                }
                newStringBuffer.append(string);
                string = newStringBuffer.toString();
            }
        }

        return string;
    }

    /**
     * The inverse operation of backQuoteChars(). Converts back-quoted carriage
     * returns and new lines in a string to the corresponding character ('\r' and
     * '\n'). Also "un"-back-quotes the following characters: ` " \ \t and %
     *
     * @param string the string
     * @return the converted string
     */
    public static String unbackQuoteChars(String string) {

        int index;
        StringBuffer newStringBuffer;

        // replace each of the following characters with the backquoted version
        String charsFind[] = { "\\\\", "\\'", "\\t", "\\n", "\\r", "\\\"", "\\%",
                "\\u001E" };
        char charsReplace[] = { '\\', '\'', '\t', '\n', '\r', '"', '%', '\u001E' };
        int pos[] = new int[charsFind.length];
        int curPos;

        String str = new String(string);
        newStringBuffer = new StringBuffer();
        while (str.length() > 0) {
            // get positions and closest character to replace
            curPos = str.length();
            index = -1;
            for (int i = 0; i < pos.length; i++) {
                pos[i] = str.indexOf(charsFind[i]);
                if ((pos[i] > -1) && (pos[i] < curPos)) {
                    index = i;
                    curPos = pos[i];
                }
            }

            // replace character if found, otherwise finished
            if (index == -1) {
                newStringBuffer.append(str);
                str = "";
            } else {
                newStringBuffer.append(str.substring(0, pos[index]));
                newStringBuffer.append(charsReplace[index]);
                str = str.substring(pos[index] + charsFind[index].length());
            }
        }

        return newStringBuffer.toString();
    }

    /**
     * Rounds a double and converts it into String.
     *
     * @param value the double value
     * @param afterDecimalPoint the (maximum) number of digits permitted after the
     *          decimal point
     * @return the double as a formatted string
     */
    public static String doubleToString(double value,
                                                   int afterDecimalPoint) {

        StringBuffer stringBuffer;
        double temp;
        int dotPosition;
        long precisionValue;

        temp = value * Math.pow(10.0, afterDecimalPoint);
        if (Math.abs(temp) < Long.MAX_VALUE) {
            precisionValue = (temp > 0) ? (long) (temp + 0.5) : -(long) (Math
                    .abs(temp) + 0.5);
            if (precisionValue == 0) {
                stringBuffer = new StringBuffer(String.valueOf(0));
            } else {
                stringBuffer = new StringBuffer(String.valueOf(precisionValue));
            }
            if (afterDecimalPoint == 0) {
                return stringBuffer.toString();
            }
            dotPosition = stringBuffer.length() - afterDecimalPoint;
            while (((precisionValue < 0) && (dotPosition < 1)) || (dotPosition < 0)) {
                if (precisionValue < 0) {
                    stringBuffer.insert(1, '0');
                } else {
                    stringBuffer.insert(0, '0');
                }
                dotPosition++;
            }
            stringBuffer.insert(dotPosition, '.');
            if ((precisionValue < 0) && (stringBuffer.charAt(1) == '.')) {
                stringBuffer.insert(1, '0');
            } else if (stringBuffer.charAt(0) == '.') {
                stringBuffer.insert(0, '0');
            }
            int currentPos = stringBuffer.length() - 1;
            while ((currentPos > dotPosition)
                    && (stringBuffer.charAt(currentPos) == '0')) {
                stringBuffer.setCharAt(currentPos--, ' ');
            }
            if (stringBuffer.charAt(currentPos) == '.') {
                stringBuffer.setCharAt(currentPos, ' ');
            }

            return stringBuffer.toString().trim();
        }
        return new String("" + value);
    }

    /**
     * Rounds a double and converts it into a formatted decimal-justified String.
     * Trailing 0's are replaced with spaces.
     *
     * @param value the double value
     * @param width the width of the string
     * @param afterDecimalPoint the number of digits after the decimal point
     * @return the double as a formatted string
     */
    public static String doubleToString(double value, int width,
                                                   int afterDecimalPoint) {

        String tempString = doubleToString(value, afterDecimalPoint);
        char[] result;
        int dotPosition;

        if ((afterDecimalPoint >= width) || (tempString.indexOf('E') != -1)) { // Protects
            // sci
            // notation
            return tempString;
        }

        // Initialize result
        result = new char[width];
        for (int i = 0; i < result.length; i++) {
            result[i] = ' ';
        }

        if (afterDecimalPoint > 0) {
            // Get position of decimal point and insert decimal point
            dotPosition = tempString.indexOf('.');
            if (dotPosition == -1) {
                dotPosition = tempString.length();
            } else {
                result[width - afterDecimalPoint - 1] = '.';
            }
        } else {
            dotPosition = tempString.length();
        }

        int offset = width - afterDecimalPoint - dotPosition;
        if (afterDecimalPoint > 0) {
            offset--;
        }

        // Not enough room to decimal align within the supplied width
        if (offset < 0) {
            return tempString;
        }

        // Copy characters before decimal point
        for (int i = 0; i < dotPosition; i++) {
            result[offset + i] = tempString.charAt(i);
        }

        // Copy characters after decimal point
        for (int i = dotPosition + 1; i < tempString.length(); i++) {
            result[offset + i] = tempString.charAt(i);
        }

        return new String(result);
    }
    /**
     * Pads a string to a specified length, inserting spaces on the left as
     * required. If the string is too long, characters are removed (from the
     * right).
     *
     * @param inString the input string
     * @param length the desired length of the output string
     * @return the output string
     */
    public static String padLeft(String inString, int length) {

        return fixStringLength(inString, length, false);
    }

    /**
     * Pads a string to a specified length, inserting spaces on the right as
     * required. If the string is too long, characters are removed (from the
     * right).
     *
     * @param inString the input string
     * @param length the desired length of the output string
     * @return the output string
     */
    public static String padRight(String inString, int length) {

        return fixStringLength(inString, length, true);
    }

    /**
     * Pads a string to a specified length, inserting spaces as required. If the
     * string is too long, characters are removed (from the right).
     *
     * @param inString the input string
     * @param length the desired length of the output string
     * @param right true if inserted spaces should be added to the right
     * @return the output string
     */
    private static String fixStringLength(String inString, int length,
                                                     boolean right) {

        if (inString.length() < length) {
            while (inString.length() < length) {
                inString = (right ? inString.concat(" ") : " ".concat(inString));
            }
        } else if (inString.length() > length) {
            inString = inString.substring(0, length);
        }
        return inString;
    }

    /**
     * Returns the value used to code a missing value. Note that equality tests on
     * this value will always return false, so use isMissingValue(double val) for
     * testing..
     *
     * @return the value used as missing value.
     */
    public static double missingValue() {

        return Double.NaN;
    }


    /**
     * Returns the logarithm of a for base 2.
     *
     * @param a a double
     * @return the logarithm for base 2
     */
    public static double log2(double a) {

        return Math.log(a) / log2;
    }

    public static int maxIndex(double[] doubles) {

        double maximum = 0;
        int maxIndex = 0;

        for (int i = 0; i < doubles.length; i++) {
            if ((i == 0) || (doubles[i] > maximum)) {
                maxIndex = i;
                maximum = doubles[i];
            }
        }

        return maxIndex;
    }

    public static int[] stableSort(double[] array) {

        int[] index = initialIndex(array.length);

        if (array.length > 1) {

            int[] newIndex = new int[array.length];
            int[] helpIndex;
            int numEqual;

            array = array.clone();
            replaceMissingWithMAX_VALUE(array);
            quickSort(array, index, 0, array.length - 1);

            // Make sort stable

            int i = 0;
            while (i < index.length) {
                numEqual = 1;
                for (int j = i + 1; ((j < index.length) && Utils.eq(array[index[i]],
                        array[index[j]])); j++) {
                    numEqual++;
                }
                if (numEqual > 1) {
                    helpIndex = new int[numEqual];
                    for (int j = 0; j < numEqual; j++) {
                        helpIndex[j] = i + j;
                    }
                    quickSort(index, helpIndex, 0, numEqual - 1);
                    for (int j = 0; j < numEqual; j++) {
                        newIndex[i + j] = index[helpIndex[j]];
                    }
                    i += numEqual;
                } else {
                    newIndex[i] = index[i];
                    i++;
                }
            }
            return newIndex;
        } else {
            return index;
        }
    }




}
