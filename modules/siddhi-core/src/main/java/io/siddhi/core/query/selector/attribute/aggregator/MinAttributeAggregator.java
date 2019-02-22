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
package io.siddhi.core.query.selector.attribute.aggregator;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.exception.OperationNotSupportedException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.query.api.definition.Attribute;

import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

/**
 * {@link AttributeAggregator} to calculate min value based on an event attribute.
 */
@Extension(
        name = "min",
        namespace = "",
        description = "Returns the minimum value for all the events.",
        parameters = {
                @Parameter(name = "arg",
                        description = "The value that needs to be compared to find the minimum value.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns the minimum value in the same type as the input.",
                type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT}),
        examples = @Example(
                syntax = "from inputStream\n" +
                        "select min(temp) as minTemp\n" +
                        "insert into outputStream;",
                description = "min(temp) returns the minimum temp value recorded for all the events based on their " +
                        "arrival and expiry."
        )
)
public class MinAttributeAggregator extends AttributeAggregator {

    private MinAttributeAggregator minOutputAttributeAggregator;

    public void init(Attribute.Type type) {
    }

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param processingMode               query processing mode
     * @param outputExpectsExpiredEvents   is expired events sent as output
     * @param configReader                 this hold the {@link MinAttributeAggregator} configuration reader.
     * @param siddhiAppContext             Siddhi app runtime context
     */
    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ProcessingMode processingMode,
                        boolean outputExpectsExpiredEvents, ConfigReader configReader,
                        SiddhiAppContext siddhiAppContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new OperationNotSupportedException("Min aggregator has to have exactly 1 parameter, currently " +
                    attributeExpressionExecutors.length + " parameters provided");
        }
        boolean trackFutureStates = false;
        if (processingMode == ProcessingMode.SLIDE || outputExpectsExpiredEvents) {
            trackFutureStates = true;
        }
        Attribute.Type type = attributeExpressionExecutors[0].getReturnType();
        switch (type) {
            case FLOAT:
                minOutputAttributeAggregator = new MinAttributeAggregatorFloat(trackFutureStates);
                break;
            case INT:
                minOutputAttributeAggregator = new MinAttributeAggregatorInt(trackFutureStates);
                break;
            case LONG:
                minOutputAttributeAggregator = new MinAttributeAggregatorLong(trackFutureStates);
                break;
            case DOUBLE:
                minOutputAttributeAggregator = new MinAttributeAggregatorDouble(trackFutureStates);
                break;
            default:
                throw new OperationNotSupportedException("Min not supported for " + type);
        }
    }

    public Attribute.Type getReturnType() {
        return minOutputAttributeAggregator.getReturnType();
    }

    @Override
    public Object processAdd(Object data) {
        if (data == null) {
            return minOutputAttributeAggregator.currentValue();
        }
        return minOutputAttributeAggregator.processAdd(data);
    }

    @Override
    public Object processAdd(Object[] data) {
        // will not occur
        return new IllegalStateException("Min cannot process data array, but found " + Arrays.deepToString(data));
    }

    @Override
    public Object processRemove(Object data) {
        if (data == null) {
            return minOutputAttributeAggregator.currentValue();
        }
        return minOutputAttributeAggregator.processRemove(data);
    }

    @Override
    public Object processRemove(Object[] data) {
        // will not occur
        return new IllegalStateException("Min cannot process data array, but found " + Arrays.deepToString(data));
    }

    protected Object currentValue() {
        return null;
    }

    @Override
    public boolean canDestroy() {
        return minOutputAttributeAggregator.canDestroy();
    }

    @Override
    public Object reset() {
        return minOutputAttributeAggregator.reset();
    }

    @Override
    public Map<String, Object> currentState() {
        return minOutputAttributeAggregator.currentState();
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        minOutputAttributeAggregator.restoreState(state);
    }

    class MinAttributeAggregatorDouble extends MinAttributeAggregator {

        private final Attribute.Type type = Attribute.Type.DOUBLE;
        private Deque<Double> minDeque = null;
        private volatile Double minValue = null;

        public MinAttributeAggregatorDouble(boolean trackFutureStates) {
            if (trackFutureStates) {
                minDeque = new LinkedList<>();
            }
        }

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public synchronized Object processAdd(Object data) {
            Double value = (Double) data;
            if (minDeque != null) {
                for (Iterator<Double> iterator = minDeque.descendingIterator(); iterator.hasNext(); ) {
                    if (iterator.next() > value) {
                        iterator.remove();
                    } else {
                        break;
                    }
                }
                minDeque.addLast(value);
            }
            if (minValue == null || minValue > value) {
                minValue = value;
            }
            return minValue;
        }

        @Override
        public synchronized Object processRemove(Object data) {
            if (minDeque != null) {
                minDeque.removeFirstOccurrence(data);
                minValue = minDeque.peekFirst();
            } else {
                if (minValue != null && minValue.equals(data)) {
                    minValue = null;
                }
            }
            return minValue;
        }

        @Override
        public synchronized Object reset() {
            if (minDeque != null) {
                minDeque.clear();
            }
            minValue = null;
            return null;
        }

        @Override
        public boolean canDestroy() {
            return (minDeque == null || minDeque.size() == 0) && minValue == null;
        }

        @Override
        public Map<String, Object> currentState() {
            Map<String, Object> state = new HashMap<>();
            synchronized (this) {
                state.put("MinValue", minValue);
                state.put("MinDeque", minDeque);
            }
            return state;
        }

        @Override
        public synchronized void restoreState(Map<String, Object> state) {
            minValue = (Double) state.get("MinValue");
            minDeque = (Deque<Double>) state.get("MinDeque");
        }

        protected Object currentValue() {
            return minValue;
        }

    }

    class MinAttributeAggregatorFloat extends MinAttributeAggregator {

        private final Attribute.Type type = Attribute.Type.FLOAT;
        private Deque<Float> minDeque = null;
        private volatile Float minValue = null;

        public MinAttributeAggregatorFloat(boolean trackFutureStates) {
            if (trackFutureStates) {
                minDeque = new LinkedList<>();
            }
        }

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public synchronized Object processAdd(Object data) {
            Float value = (Float) data;
            if (minDeque != null) {
                for (Iterator<Float> iterator = minDeque.descendingIterator(); iterator.hasNext(); ) {
                    if (iterator.next() > value) {
                        iterator.remove();
                    } else {
                        break;
                    }
                }
                minDeque.addLast(value);
            }
            if (minValue == null || minValue > value) {
                minValue = value;
            }
            return minValue;
        }

        @Override
        public synchronized Object processRemove(Object data) {
            if (minDeque != null) {
                minDeque.removeFirstOccurrence(data);
                minValue = minDeque.peekFirst();
            } else {
                if (minValue != null && minValue.equals(data)) {
                    minValue = null;
                }
            }
            return minValue;
        }

        @Override
        public synchronized Object reset() {
            if (minDeque != null) {
                minDeque.clear();
            }
            minValue = null;
            return null;
        }

        @Override
        public boolean canDestroy() {
            return (minDeque == null || minDeque.size() == 0) && minValue == null;
        }

        @Override
        public Map<String, Object> currentState() {
            Map<String, Object> state = new HashMap<>();
            synchronized (this) {
                state.put("MinValue", minValue);
                state.put("MinDeque", minDeque);
            }
            return state;
        }

        @Override
        public synchronized void restoreState(Map<String, Object> state) {
            minValue = (Float) state.get("MinValue");
            minDeque = (Deque<Float>) state.get("MinDeque");
        }

        protected Object currentValue() {
            return minValue;
        }
    }

    class MinAttributeAggregatorInt extends MinAttributeAggregator {

        private final Attribute.Type type = Attribute.Type.INT;
        private Deque<Integer> minDeque = null;
        private volatile Integer minValue = null;

        public MinAttributeAggregatorInt(boolean trackFutureStates) {
            if (trackFutureStates) {
                minDeque = new LinkedList<>();
            }
        }

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public synchronized Object processAdd(Object data) {
            Integer value = (Integer) data;
            if (minDeque != null) {
                for (Iterator<Integer> iterator = minDeque.descendingIterator(); iterator.hasNext(); ) {

                    if (iterator.next() > value) {
                        iterator.remove();
                    } else {
                        break;
                    }
                }
                minDeque.addLast(value);
            }
            if (minValue == null || minValue > value) {
                minValue = value;
            }
            return minValue;
        }

        @Override
        public synchronized Object reset() {
            if (minDeque != null) {
                minDeque.clear();
            }
            minValue = null;
            return null;
        }

        @Override
        public boolean canDestroy() {
            return (minDeque == null || minDeque.size() == 0) && minValue == null;
        }

        @Override
        public synchronized Object processRemove(Object data) {
            if (minDeque != null) {
                minDeque.removeFirstOccurrence(data);
                minValue = minDeque.peekFirst();
            } else {
                if (minValue != null && minValue.equals(data)) {
                    minValue = null;
                }
            }
            return minValue;
        }

        @Override
        public Map<String, Object> currentState() {
            Map<String, Object> state = new HashMap<>();
            synchronized (this) {
                state.put("MinValue", minValue);
                state.put("MinDeque", minDeque);
            }
            return state;
        }

        @Override
        public synchronized void restoreState(Map<String, Object> state) {
            minValue = (Integer) state.get("MinValue");
            minDeque = (Deque<Integer>) state.get("MinDeque");
        }

        protected Object currentValue() {
            return minValue;
        }
    }

    class MinAttributeAggregatorLong extends MinAttributeAggregator {

        private final Attribute.Type type = Attribute.Type.LONG;
        private Deque<Long> minDeque = null;
        private volatile Long minValue = null;

        public MinAttributeAggregatorLong(boolean trackFutureStates) {
            if (trackFutureStates) {
                minDeque = new LinkedList<>();
            }
        }

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public synchronized Object processAdd(Object data) {
            Long value = (Long) data;
            if (minDeque != null) {
                for (Iterator<Long> iterator = minDeque.descendingIterator(); iterator.hasNext(); ) {
                    if (iterator.next() > value) {
                        iterator.remove();
                    } else {
                        break;
                    }
                }
                minDeque.addLast(value);
            }
            if (minValue == null || minValue > value) {
                minValue = value;
            }
            return minValue;
        }

        @Override
        public synchronized Object reset() {
            if (minDeque != null) {
                minDeque.clear();
            }
            minValue = null;
            return null;
        }

        @Override
        public boolean canDestroy() {
            return (minDeque == null || minDeque.size() == 0) && minValue == null;
        }

        @Override
        public synchronized Object processRemove(Object data) {
            if (minDeque != null) {
                minDeque.removeFirstOccurrence(data);
                minValue = minDeque.peekFirst();
            } else {
                if (minValue != null && minValue.equals(data)) {
                    minValue = null;
                }
            }
            return minValue;
        }

        @Override
        public Map<String, Object> currentState() {
            Map<String, Object> state = new HashMap<>();
            synchronized (this) {
                state.put("MinValue", minValue);
                state.put("MinDeque", minDeque);
            }
            return state;
        }

        @Override
        public synchronized void restoreState(Map<String, Object> state) {
            minValue = (Long) state.get("MinValue");
            minDeque = (Deque<Long>) state.get("MinDeque");
        }

        protected Object currentValue() {
            return minValue;
        }

    }

}
