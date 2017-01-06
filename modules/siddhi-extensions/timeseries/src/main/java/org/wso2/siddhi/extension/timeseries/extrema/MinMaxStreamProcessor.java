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

package org.wso2.siddhi.extension.timeseries.extrema;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/*
 * This class finds minimum and/or maximum value within a given length window (l+L), where following conditions are met.
 *
 * For minimum:
 * an event at least d% greater than minimum event must have arrived prior to minimum, within an l length window
 * an event at least D% greater than minimum event must arrive after minimum, within L length window
 *
 * For maximum:
 * an event at least d% less than maximum event must have arrived prior to maximum, within an l length window
 * an event at least D% less than maximum event must arrive after maximum, within L length window
 *
 * Sample Query (variable, l, L, d, D, extrema type):
 * from inputStream#timeseries:minMax(price, 4, 4, 1, 2, 'minmax')
 * select *
 * insert into outputStream;
 */
public class MinMaxStreamProcessor extends StreamProcessor {
    private ExtremaType extremaType; // Whether to find min and/or max
    private LinkedList<StreamEvent> eventStack = null; // Stores all the events within l + L window
    private LinkedList<valueObject> valueStack = null; // Stores all the values within l + L window
    private valueObject valueRemoved = null; // Expired event
    private Deque<valueObject> maxDeque = new LinkedList<valueObject>(); // Stores all the values which could
                                                               // be next max (including current max)
    private Deque<valueObject> minDeque = new LinkedList<valueObject>(); // Stores all the values which could
                                                               // be next min (including current min)
    private valueObject currentMax = null; // Current max (before testing d, D conditions)
    private valueObject currentMin = null; // Current min (before testing d, D conditions)
    private int[] variablePosition; // Position of variable to be executed
    private int l; // l window length
    private int L; // L window length
    private double d; // d percentage
    private double D; // D percentage

    private enum ExtremaType {
        MIN, MAX, MINMAX
    }

    /**
     * The init method of MinMaxStreamProcessor,
     * this method will be called before other methods
     *
     * Input parameters:
     * 1st parameter: variable
     * 2nd parameter: l window length
     * 3rd parameter: L window length
     * 4th parameter: d percentage
     * 5th parameter: D percentage
     * 6th parameter: extrema type
     *
     * Additional output attributes:
     * extremaType
     * actual_l, actual_L: distance from min/max to where the threshold values occur (d%, D% values)
     *
     * @param inputDefinition the incoming stream definition
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param executionPlanContext the context of the execution plan
     * @return the additional output attributes (extremaType, actual_l, actual_L) introduced by the function
     */
    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition,
            ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.length != 6) {
            throw new ExecutionPlanValidationException(
                    "Invalid no of arguments passed to MinMaxStreamProcessor, required 6, but found "
                            + attributeExpressionExecutors.length);
        }
        if (!(attributeExpressionExecutors[0] instanceof VariableExpressionExecutor)) {
            throw new ExecutionPlanValidationException("MinMaxStreamProcessor's 1st parameter should"
                    + " be a variable, but found " + attributeExpressionExecutors[0].getClass());
        }
        if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
            throw new ExecutionPlanValidationException("Constant value expected as the 2nd parameter (l)"
                    + " but found " + attributeExpressionExecutors[1].getClass());
        }
        if (!(attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor)) {
            throw new ExecutionPlanValidationException("Constant value expected as the 3rd parameter (L)"
                    + " but found " + attributeExpressionExecutors[2].getClass());
        }
        if (!(attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor)) {
            throw new ExecutionPlanValidationException("Constant value expected as the 4th parameter (d)"
                    + " but found " + attributeExpressionExecutors[3].getClass());
        }
        if (!(attributeExpressionExecutors[4] instanceof ConstantExpressionExecutor)) {
            throw new ExecutionPlanValidationException("Constant value expected as the 5th parameter (D)"
                    + " but found " + attributeExpressionExecutors[4].getClass());
        }
        if (!(attributeExpressionExecutors[5] instanceof ConstantExpressionExecutor)) {
            throw new ExecutionPlanValidationException("Constant value expected as the 6th parameter (extrema type)"
                    + " but found " + attributeExpressionExecutors[5].getClass());
        }
        if (!(attributeExpressionExecutors[0].getReturnType() == Attribute.Type.DOUBLE
                || attributeExpressionExecutors[0].getReturnType() == Attribute.Type.INT
                || attributeExpressionExecutors[0].getReturnType() == Attribute.Type.FLOAT
                || attributeExpressionExecutors[0].getReturnType() == Attribute.Type.LONG)) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the 1st argument (variable) of MinMaxStreamProcessor, "
                            + "required " + Attribute.Type.DOUBLE + " or " + Attribute.Type.FLOAT + " or "
                            + Attribute.Type.INT + " or " + Attribute.Type.LONG + " but found "
                            + attributeExpressionExecutors[0].getReturnType().toString());
        }
        variablePosition = ((VariableExpressionExecutor) attributeExpressionExecutors[0]).getPosition();
        try {
            l = Integer.parseInt(
                    String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue()));
        } catch (NumberFormatException e) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the 2nd argument (l) of MinMaxStreamProcessor " + "required "
                            + Attribute.Type.INT + " constant, but found "
                            + attributeExpressionExecutors[1].getReturnType().toString());
        }
        try {
            L = Integer.parseInt(
                    String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue()));
        } catch (NumberFormatException e) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the 3rd argument (L) of MinMaxStreamProcessor " + "required "
                            + Attribute.Type.INT + " constant, but found "
                            + attributeExpressionExecutors[2].getReturnType().toString());
        }
        try {
            d = Double.parseDouble(
                    String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[3]).getValue()));
        } catch (NumberFormatException e) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the 4th argument (d) of MinMaxStreamProcessor " + "required "
                            + Attribute.Type.DOUBLE + " constant, but found "
                            + attributeExpressionExecutors[3].getReturnType().toString());
        }
        try {
            D = Double.parseDouble(
                    String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[4]).getValue()));
        } catch (NumberFormatException e) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the 5th argument (D) of MinMaxStreamProcessor " + "required "
                            + Attribute.Type.DOUBLE + " constant, but found "
                            + attributeExpressionExecutors[4].getReturnType().toString());
        }
        String extremaType = (String) ((ConstantExpressionExecutor) attributeExpressionExecutors[5]).getValue();

        if ("min".equalsIgnoreCase(extremaType)) {
            this.extremaType = ExtremaType.MIN;
        } else if ("max".equalsIgnoreCase(extremaType)) {
            this.extremaType = ExtremaType.MAX;
        } else {
            this.extremaType = ExtremaType.MINMAX;
        }

        eventStack = new LinkedList<StreamEvent>();
        valueStack = new LinkedList<valueObject>();

        List<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(new Attribute("extremaType", Attribute.Type.STRING));
        attributeList.add(new Attribute("actual_l", Attribute.Type.INT));
        attributeList.add(new Attribute("actual_L", Attribute.Type.INT));
        return attributeList;
    }

    /**
     * The main processing method that will be called upon event arrival
     *
     * @param streamEventChunk the event chunk that need to be processed
     * @param nextProcessor the next processor to which the success events need to be passed
     * @param streamEventCloner helps to clone the incoming event for local storage or modification
     * @param complexEventPopulater helps to populate the events with the resultant attributes
     */
    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
            StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        ComplexEventChunk<StreamEvent> returnEventChunk = new ComplexEventChunk<StreamEvent>(false);
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent event = streamEventChunk.next();
                streamEventChunk.remove();

                // Variable value of the latest event
                double value = Double.parseDouble(String.valueOf(event.getAttribute(variablePosition)));
                eventStack.add(event);

                // Create an object holding latest event value and insert into valueStack and valueStackLastL
                valueObject newInput = new valueObject();
                newInput.setValue(value);
                valueStack.add(newInput);

                switch (extremaType) {
                case MINMAX:
                    // Retain only l+L events
                    if (eventStack.size() > l + L) {
                        eventStack.remove();
                        valueRemoved = valueStack.remove();
                    }
                    currentMax = maxDequeIterator(newInput);
                    currentMin = minDequeIterator(newInput);
                    // Find whether current max satisfies d, D conditions and output value if so
                    if (newInput.getValue() <= currentMax.getMaxThreshold() && !currentMax.isOutputAsRealMax()
                            && currentMax.isEligibleForRealMax()) {
                        StreamEvent returnEvent = findIfActualMax(newInput);
                        if (returnEvent != null) {
                            returnEventChunk.add(returnEvent);
                        }
                    }
                    // Find whether current min satisfies d, D conditions and output value if so
                    if (newInput.getValue() >= currentMin.getMinThreshold() && !currentMin.isOutputAsRealMin()
                            && currentMin.isEligibleForRealMin()) {
                        StreamEvent returnEvent = findIfActualMin(newInput);
                        if (returnEvent != null) {
                            returnEventChunk.add(returnEvent);
                        }
                    }
                    break;
                case MAX:
                    // Retain only l+L events
                    if (eventStack.size() > l + L) {
                        eventStack.remove();
                        valueRemoved = valueStack.remove();
                    }
                    currentMax = maxDequeIterator(newInput);
                    // Find whether current max satisfies d, D conditions and output value if so
                    if (newInput.getValue() <= currentMax.getMaxThreshold() && !currentMax.isOutputAsRealMax()
                            && currentMax.isEligibleForRealMax()) {
                        StreamEvent returnEvent = findIfActualMax(newInput);
                        if (returnEvent != null) {
                            returnEventChunk.add(returnEvent);
                        }
                    }
                    break;
                case MIN:
                    // Retain only l+L events
                    if (eventStack.size() > l + L) {
                        eventStack.remove();
                        valueRemoved = valueStack.remove();
                    }
                    currentMin = minDequeIterator(newInput);
                    // Find whether current min satisfies d, D conditions and output value if so
                    if (newInput.getValue() >= currentMin.getMinThreshold() && !currentMin.isOutputAsRealMin()
                            && currentMin.isEligibleForRealMin()) {
                        StreamEvent returnEvent = findIfActualMin(newInput);
                        if (returnEvent != null) {
                            returnEventChunk.add(returnEvent);
                        }
                    }
                    break;
                }
            }
        }
        nextProcessor.process(returnEventChunk);
    }

    /**
     * Method to find whether a value d% greater than or equal to min exists within l length window,
     * by looping through older events. It further verifies whether D condition is met within L window.
     * This method is called only if
     * the latest event satisfies the D condition &&
     * current min has not already been sent as output &&
     * current min has not failed d, L condition previously
     *
     * @param latestEvent object holding value of latest event
     * @return if d, L conditions are met, send stream event output with
     *         extrema type,
     *         actual_l (distance at which a value satisfying d condition is found),
     *         actual_L (distance at which a value satisfying D condition is found)
     */
    private StreamEvent findIfActualMin(valueObject latestEvent) {
        int indexCurrentMin = valueStack.indexOf(currentMin);
        int actual_L = valueStack.indexOf(latestEvent) - indexCurrentMin;
        // If latest event is at a distance greater than L from min, min is not eligible to be sent as output
        if (actual_L > L) {
            currentMin.notEligibleForRealMin();
            return null;
        }
        int actual_l = 1;
        double dThreshold = currentMin.getValue() + currentMin.getValue() * d / 100;
        while (actual_l <= l && indexCurrentMin - actual_l >= 0) {
            if (valueStack.get(indexCurrentMin - actual_l).getValue() >= dThreshold) {
                StreamEvent outputEvent = eventStack.get(indexCurrentMin);
                complexEventPopulater.populateComplexEvent(outputEvent, new Object[] { "min", actual_l, actual_L });
                currentMin.sentOutputAsRealMin();
                return outputEvent;
            }
            ++actual_l;
        }
        // Completed iterating through l older events. No events which satisfy d condition found.
        // Therefore min is not eligible to be sent as output.
        currentMin.notEligibleForRealMin();
        return null;
    }

    /**
     * Method to find whether a value d% less than or equal to max exists within l length window,
     * by looping through older events. It further verifies whether D condition is met within L window.
     * This method is called only if
     * the latest event satisfies the D condition &&
     * current max has not already been sent as output &&
     * current max has not failed d, L condition previously
     *
     * @param latestEvent object holding value of latest event
     * @return if d, L conditions are met, send stream event output with
     *         extrema type,
     *         actual_l (distance at which a value satisfying d condition is found),
     *         actual_L (distance at which a value satisfying D condition is found)
     */
    private StreamEvent findIfActualMax(valueObject latestEvent) {
        int indexCurrentMax = valueStack.indexOf(currentMax);
        int actual_L = valueStack.indexOf(latestEvent) - indexCurrentMax;
        // If latest event is at a distance greater than L from max, max is not eligible to be sent as output
        if (actual_L > L) {
            currentMax.notEligibleForRealMax();
            return null;
        }
        int actual_l = 1;
        double dThreshold = currentMax.getValue() - currentMax.getValue() * d / 100;
        while (actual_l <= l && indexCurrentMax - actual_l >= 0) {
            if (valueStack.get(indexCurrentMax - actual_l).getValue() <= dThreshold) {
                StreamEvent outputEvent = eventStack.get(indexCurrentMax);
                complexEventPopulater.populateComplexEvent(outputEvent, new Object[] { "max", actual_l, actual_L });
                currentMax.sentOutputAsRealMax();
                return outputEvent;
            }
            ++actual_l;
        }
        // Completed iterating through l older events. No events which satisfy d condition found.
        // Therefore max is not eligible to be sent as output.
        currentMax.notEligibleForRealMax();
        return null;
    }

    /**
     * This method stores all the values possible to become next max, with current max (largest value)
     * at the head. The value expiring from l + L window is removed if it's in maxDeque
     *
     * @param valObject latest incoming value
     * @return maximum value (without checking d, D conditions)
     */
    private valueObject maxDequeIterator(valueObject valObject) {
        if (valueRemoved != null) {
            for (Iterator<valueObject> iterator = maxDeque.descendingIterator(); iterator.hasNext();) {
                double possibleMaxValue = iterator.next().getValue();
                if (possibleMaxValue < valObject.getValue() || possibleMaxValue <= valueRemoved.getValue()) {
                    if (possibleMaxValue < valObject.getValue()) {
                        iterator.remove();
                    } else if (valueRemoved.getValue() == possibleMaxValue) {
                        // If expired value is in maxDeque, it must be removed
                        iterator.remove();
                        break;
                    }
                } else {
                    break;
                }
            }
        } else {
            for (Iterator<valueObject> iterator = maxDeque.descendingIterator(); iterator.hasNext();) {
                if (iterator.next().getValue() < valObject.getValue()) {
                    iterator.remove();
                } else {
                    break;
                }
            }
        }
        valObject.setMaxThreshold();
        maxDeque.addLast(valObject);
        return maxDeque.peek();
    }

    /**
     * This method stores all the values possible to become next min, with current min (minimum value)
     * at the head. The value expiring from l + L window is removed if it's in minDeque
     *
     * @param valObject latest incoming value
     * @return minimum value (without checking d, D conditions)
     */
    private valueObject minDequeIterator(valueObject valObject) {
        if (valueRemoved != null) {
            for (Iterator<valueObject> iterator = minDeque.descendingIterator(); iterator.hasNext();) {
                double possibleMinValue = iterator.next().getValue();
                if (possibleMinValue > valObject.getValue() || possibleMinValue >= valueRemoved.getValue()) {
                    if (possibleMinValue > valObject.getValue()) {
                        iterator.remove();
                    } else if (valueRemoved.getValue() == possibleMinValue) {
                        // If removed value is in minDeque, it must be removed
                        iterator.remove();
                        break;
                    }
                } else {
                    break;
                }
            }
        } else {
            for (Iterator<valueObject> iterator = minDeque.descendingIterator(); iterator.hasNext();) {
                if (iterator.next().getValue() > valObject.getValue()) {
                    iterator.remove();
                } else {
                    break;
                }
            }
        }
        valObject.setMinThreshold();
        minDeque.addLast(valObject);
        return minDeque.peek();
    }

    /**
     * No resources to acquire. Therefore no method implementation
     */
    @Override
    public void start() {

    }

    /**
     * No resources to release. Therefore no method implementation
     */
    @Override
    public void stop() {

    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted to reconstruct the element to the same state on a different point of time
     *
     * @return stateful objects of the processing element as an array
     */
    @Override
    public Object[] currentState() {
        return new Object[] { eventStack, valueStack, maxDeque, minDeque, valueRemoved, currentMax, currentMin };
    }

    /**
     * Used to restore serialized state of the processing element, for reconstructing
     * the element to the same state as if was on a previous point of time.
     *
     * @param state the stateful objects of the element as an array on the same order provided by currentState().
     */
    @Override
    public void restoreState(Object[] state) {
        eventStack = (LinkedList<StreamEvent>) state[0];
        valueStack = (LinkedList<valueObject>) state[1];
        maxDeque = (Deque<valueObject>) state[2];
        minDeque = (Deque<valueObject>) state[3];
        valueRemoved = (valueObject) state[4];
        currentMax = (valueObject) state[5];
        currentMin = (valueObject) state[6];
    }

    /*
     * The value object class which holds additional information which is useful in finding
     * whether min/max is eligible to be sent as output
     */
    private class valueObject {
        private double value; // Variable value
        private double minThreshold; // If event is min, the D threshold to consider
        private double maxThreshold; // If event is max, the D threshold to consider
        private boolean eligibleForRealMax = true; // Max eligibility based on d, L conditions.
                                                   // Initially set to true
        private boolean eligibleForRealMin = true; // Min eligibility based on d, L conditions.
        private boolean outputAsRealMin = false; // Whether event was already sent as min output
        private boolean outputAsRealMax = false; // Whether event was already sent as max output

        /**
         * Method to set value
         *
         * @param value variable value
         */
        private void setValue(double value) {
            this.value = value;
        }

        /**
         * Method to return value
         *
         * @return object value
         */
        private double getValue() {
            return value;
        }

        /**
         * Method to set threshold value which satisfies D condition, if this object becomes min
         */
        private void setMinThreshold() {
            minThreshold = value + value * D / 100;
        }

        /**
         * Method to set threshold value which satisfies D condition, if this object becomes max
         */
        private void setMaxThreshold() {
            maxThreshold = value - value * D / 100;
        }

        /**
         * Method to return threshold. If this object becomes min, compare it's threshold with latest event
         * 
         * @return threshold satisfying D condition
         */
        private double getMinThreshold() {
            return minThreshold;
        }

        /**
         * Method to return threshold. If this object becomes max, compare it's threshold with latest event
         * 
         * @return threshold satisfying D condition
         */
        private double getMaxThreshold() {
            return maxThreshold;
        }

        /**
         * If d condition is checked when this object is max, and it fails,
         * the object can be set as not eligible to be sent as max output.
         * Furthermore, if D condition is not met within L events same can be done.
         */
        private void notEligibleForRealMax() {
            eligibleForRealMax = false;
        }

        /**
         * If d condition is checked when this object is min, and it fails,
         * the object can be set as not eligible to be sent as min output.
         * Furthermore, if D condition is not met within L events same can be done.
         */
        private void notEligibleForRealMin() {
            eligibleForRealMin = false;
        }

        /**
         * Method to return max eligibility based on d, L condition
         * 
         * @return eligibility to become max
         */
        private boolean isEligibleForRealMax() {
            return eligibleForRealMax;
        }

        /**
         * Method to return min eligibility based on d, L condition
         * 
         * @return eligibility to become min
         */
        private boolean isEligibleForRealMin() {
            return eligibleForRealMin;
        }

        /**
         * If this object was already sent as min output, set
         * outputAsRealMin = true
         */
        private void sentOutputAsRealMin() {
            outputAsRealMin = true;
        }

        /**
         * If this object was already sent as max output, set
         * outputAsRealMax = true
         */
        private void sentOutputAsRealMax() {
            outputAsRealMax = true;
        }

        /**
         * Check whether this object was already sent as min output. If so no need to check twice
         * 
         * @return whether object was already sent as min output
         */
        private boolean isOutputAsRealMin() {
            return outputAsRealMin;
        }

        /**
         * Check whether this object was already sent as max output. If so no need to check twice
         * 
         * @return whether object was already sent as max output
         */
        private boolean isOutputAsRealMax() {
            return outputAsRealMax;
        }
    }
}
