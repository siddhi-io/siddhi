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
import io.siddhi.annotation.ParameterOverload;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.exception.OperationNotSupportedException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;

import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

/**
 * {@link AttributeAggregatorExecutor} to calculate max value based on an event attribute.
 */
@Extension(
        name = "max",
        namespace = "",
        description = "Returns the maximum value for all the events.",
        parameters = {
                @Parameter(name = "arg",
                        description = "The value that needs to be compared to find the maximum value.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT},
                        dynamic = true)
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"arg"})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns the maximum value in the same data type as the input.",
                type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT}),
        examples = @Example(
                syntax = "from fooStream#window.timeBatch(10 sec)\n" +
                        "select max(temp) as maxTemp\n" +
                        "insert into barStream;",
                description = "max(temp) returns the maximum temp value recorded for all the events based on their " +
                        "arrival and expiry."
        )
)
public class MaxAttributeAggregatorExecutor
        extends AttributeAggregatorExecutor<MaxAttributeAggregatorExecutor.MaxAggregatorState> {

    private Attribute.Type returnType;
    private ExpressionExecutor attributeExpressionExecutor;

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param processingMode               query processing mode
     * @param outputExpectsExpiredEvents   is expired events sent as output
     * @param configReader                 this hold the {@link MaxAttributeAggregatorExecutor} configuration reader.
     * @param siddhiQueryContext           Siddhi query runtime context
     */
    @Override
    protected StateFactory<MaxAggregatorState> init(ExpressionExecutor[] attributeExpressionExecutors,
                                                    ProcessingMode processingMode,
                                                    boolean outputExpectsExpiredEvents,
                                                    ConfigReader configReader,
                                                    SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new OperationNotSupportedException("Max aggregator has to have exactly 1 parameter, currently " +
                    attributeExpressionExecutors.length + " parameters provided");
        }
        boolean trackFutureStates = false;
        if (processingMode == ProcessingMode.SLIDE || outputExpectsExpiredEvents) {
            trackFutureStates = true;
        }
        attributeExpressionExecutor = attributeExpressionExecutors[0];
        returnType = attributeExpressionExecutors[0].getReturnType();
        boolean finalTrackFutureStates = trackFutureStates;
        return () -> {
            switch (returnType) {
                case FLOAT:
                    return new MaxAttributeAggregatorStateFloat(finalTrackFutureStates);
                case INT:
                    return new MaxAttributeAggregatorStateInt(finalTrackFutureStates);
                case LONG:
                    return new MaxAttributeAggregatorStateLong(finalTrackFutureStates);
                case DOUBLE:
                    return new MaxAttributeAggregatorStateDouble(finalTrackFutureStates);
                default:
                    throw new OperationNotSupportedException("Max not supported for " + returnType);
            }
        };
    }

    public Attribute.Type getReturnType() {
        return returnType;
    }

    public ExpressionExecutor getAttributeExpressionExecutor() {
        return attributeExpressionExecutor;
    }

    @Override
    public Object processAdd(Object data, MaxAggregatorState state) {
        if (data == null) {
            return state.currentValue();
        }
        return state.processAdd(data);
    }

    @Override
    public Object processAdd(Object[] data, MaxAggregatorState state) {
        // will not occur
        return new IllegalStateException("Max cannot process data array, but found " + Arrays.deepToString(data));
    }

    @Override
    public Object processRemove(Object data, MaxAggregatorState state) {
        if (data == null) {
            return state.currentValue();
        }
        return state.processRemove(data);
    }

    @Override
    public Object processRemove(Object[] data, MaxAggregatorState state) {
        // will not occur
        return new IllegalStateException("Max cannot process data array, but found " + Arrays.deepToString(data));
    }

    @Override
    public Object reset(MaxAggregatorState state) {
        return state.reset();
    }

    class MaxAttributeAggregatorStateDouble extends MaxAggregatorState {

        private Deque<Double> maxDeque = null;
        private volatile Double maxValue = null;

        public MaxAttributeAggregatorStateDouble(boolean trackFutureStates) {
            if (trackFutureStates) {
                maxDeque = new LinkedList<>();
            }
        }

        @Override
        public Object processAdd(Object data) {
            Double value = (Double) data;
            if (maxDeque != null) {
                for (Iterator<Double> iterator = maxDeque.descendingIterator(); iterator.hasNext(); ) {
                    if (iterator.next() < value) {
                        iterator.remove();
                    } else {
                        break;
                    }
                }
                maxDeque.addLast(value);
            }
            if (maxValue == null || maxValue < value) {
                maxValue = value;
            }
            return maxValue;
        }

        @Override
        public Object processRemove(Object data) {
            if (maxDeque != null) {
                maxDeque.removeFirstOccurrence(data);
                maxValue = maxDeque.peekFirst();
            } else {
                if (maxValue != null && maxValue.equals(data)) {
                    maxValue = null;
                }
            }
            return maxValue;
        }

        @Override
        public Object reset() {
            if (maxDeque != null) {
                maxDeque.clear();
            }
            maxValue = null;
            return null;
        }

        @Override
        public boolean canDestroy() {
            return (maxDeque == null || maxDeque.isEmpty()) && maxValue == null;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("MaxValue", maxValue);
            state.put("MaxDeque", maxDeque);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            maxValue = (Double) state.get("MaxValue");
            maxDeque = (Deque<Double>) state.get("MaxDeque");
        }

        protected Object currentValue() {
            return maxValue;
        }
    }

    class MaxAttributeAggregatorStateFloat extends MaxAggregatorState {

        private Deque<Float> maxDeque = null;
        private volatile Float maxValue = null;

        public MaxAttributeAggregatorStateFloat(boolean trackFutureStates) {
            if (trackFutureStates) {
                maxDeque = new LinkedList<>();
            }
        }

        @Override
        public Object processAdd(Object data) {
            Float value = (Float) data;
            if (maxDeque != null) {
                for (Iterator<Float> iterator = maxDeque.descendingIterator(); iterator.hasNext(); ) {
                    if (iterator.next() < value) {
                        iterator.remove();
                    } else {
                        break;
                    }
                }
                maxDeque.addLast(value);
            }
            if (maxValue == null || maxValue < value) {
                maxValue = value;
            }
            return maxValue;
        }

        @Override
        public Object processRemove(Object data) {
            if (maxDeque != null) {
                maxDeque.removeFirstOccurrence(data);
                maxValue = maxDeque.peekFirst();
            } else {
                if (maxValue != null && maxValue.equals(data)) {
                    maxValue = null;
                }
            }
            return maxValue;
        }

        @Override
        public Object reset() {
            if (maxDeque != null) {
                maxDeque.clear();
            }
            maxValue = null;
            return null;
        }

        @Override
        public boolean canDestroy() {
            return (maxDeque == null || maxDeque.isEmpty()) && maxValue == null;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("MaxValue", maxValue);
            state.put("MaxDeque", maxDeque);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            maxValue = (Float) state.get("MaxValue");
            maxDeque = (Deque<Float>) state.get("MaxDeque");
        }

        protected Object currentValue() {
            return maxValue;
        }

    }

    class MaxAttributeAggregatorStateInt extends MaxAggregatorState {

        private Deque<Integer> maxDeque = null;
        private volatile Integer maxValue = null;

        public MaxAttributeAggregatorStateInt(boolean trackFutureStates) {
            if (trackFutureStates) {
                maxDeque = new LinkedList<>();
            }
        }

        @Override
        public Object processAdd(Object data) {
            Integer value = (Integer) data;
            if (maxDeque != null) {
                for (Iterator<Integer> iterator = maxDeque.descendingIterator(); iterator.hasNext(); ) {
                    if (iterator.next() < value) {
                        iterator.remove();
                    } else {
                        break;
                    }
                }
                maxDeque.addLast(value);
            }
            if (maxValue == null || maxValue < value) {
                maxValue = value;
            }
            return maxValue;
        }

        @Override
        public Object processRemove(Object data) {
            if (maxDeque != null) {
                maxDeque.removeFirstOccurrence(data);
                maxValue = maxDeque.peekFirst();
            } else {
                if (maxValue != null && maxValue.equals(data)) {
                    maxValue = null;
                }
            }
            return maxValue;
        }

        @Override
        public Object reset() {
            if (maxDeque != null) {
                maxDeque.clear();
            }
            maxValue = null;
            return null;
        }

        @Override
        public boolean canDestroy() {
            return (maxDeque == null || maxDeque.isEmpty()) && maxValue == null;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("MaxValue", maxValue);
            state.put("MaxDeque", maxDeque);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            maxValue = (Integer) state.get("MaxValue");
            maxDeque = (Deque<Integer>) state.get("MaxDeque");
        }

        protected Object currentValue() {
            return maxValue;
        }

    }

    class MaxAttributeAggregatorStateLong extends MaxAggregatorState {

        private Deque<Long> maxDeque = null;
        private volatile Long maxValue = null;

        public MaxAttributeAggregatorStateLong(boolean trackFutureStates) {
            if (trackFutureStates) {
                maxDeque = new LinkedList<>();
            }
        }

        @Override
        public Object processAdd(Object data) {
            Long value = (Long) data;
            if (maxDeque != null) {
                for (Iterator<Long> iterator = maxDeque.descendingIterator(); iterator.hasNext(); ) {
                    if (iterator.next() < value) {
                        iterator.remove();
                    } else {
                        break;
                    }
                }
                maxDeque.addLast(value);
            }
            if (maxValue == null || maxValue < value) {
                maxValue = value;
            }
            return maxValue;
        }

        @Override
        public Object processRemove(Object data) {
            if (maxDeque != null) {
                maxDeque.removeFirstOccurrence(data);
                maxValue = maxDeque.peekFirst();
            } else {
                if (maxValue != null && maxValue.equals(data)) {
                    maxValue = null;
                }
            }
            return maxValue;
        }

        @Override
        public Object reset() {
            if (maxDeque != null) {
                maxDeque.clear();
            }
            maxValue = null;
            return null;
        }

        @Override
        public boolean canDestroy() {
            return (maxDeque == null || maxDeque.isEmpty()) && maxValue == null;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("MaxValue", maxValue);
            state.put("MaxDeque", maxDeque);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            maxValue = (Long) state.get("MaxValue");
            maxDeque = (Deque<Long>) state.get("MaxDeque");
        }

        protected Object currentValue() {
            return maxValue;
        }

    }

    abstract class MaxAggregatorState extends State {
        public abstract Object processAdd(Object data);

        public abstract Object processRemove(Object data);

        public abstract Object reset();

        protected abstract Object currentValue();
    }

}
