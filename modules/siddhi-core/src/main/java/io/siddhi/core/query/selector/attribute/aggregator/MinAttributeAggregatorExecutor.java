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
 * {@link AttributeAggregatorExecutor} to calculate min value based on an event attribute.
 */
@Extension(
        name = "min",
        namespace = "",
        description = "Returns the minimum value for all the events.",
        parameters = {
                @Parameter(name = "arg",
                        description = "The value that needs to be compared to find the minimum value.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT},
                        dynamic = true)
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"arg"})
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
public class MinAttributeAggregatorExecutor
        extends AttributeAggregatorExecutor<MinAttributeAggregatorExecutor.MinAggregatorState> {

    private Attribute.Type returnType;

    public void init(Attribute.Type type) {
    }

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param processingMode               query processing mode
     * @param outputExpectsExpiredEvents   is expired events sent as output
     * @param configReader                 this hold the {@link MinAttributeAggregatorExecutor} configuration reader.
     * @param siddhiQueryContext           Siddhi query runtime context
     */
    @Override
    protected StateFactory<MinAggregatorState> init(ExpressionExecutor[] attributeExpressionExecutors,
                                                    ProcessingMode processingMode,
                                                    boolean outputExpectsExpiredEvents, ConfigReader configReader,
                                                    SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new OperationNotSupportedException("Min aggregator has to have exactly 1 parameter, currently " +
                    attributeExpressionExecutors.length + " parameters provided");
        }
        boolean trackFutureStates = false;
        if (processingMode == ProcessingMode.SLIDE || outputExpectsExpiredEvents) {
            trackFutureStates = true;
        }
        returnType = attributeExpressionExecutors[0].getReturnType();
        boolean finalTrackFutureStates = trackFutureStates;
        return new StateFactory<MinAggregatorState>() {
            @Override
            public MinAggregatorState createNewState() {
                switch (returnType) {
                    case FLOAT:
                        return new MinAttributeAggregatorStateFloat(finalTrackFutureStates);
                    case INT:
                        return new MinAttributeAggregatorStateInt(finalTrackFutureStates);
                    case LONG:
                        return new MinAttributeAggregatorStateLong(finalTrackFutureStates);
                    case DOUBLE:
                        return new MinAttributeAggregatorStateDouble(finalTrackFutureStates);
                    default:
                        throw new OperationNotSupportedException("Min not supported for " + returnType);
                }
            }
        };

    }

    public Attribute.Type getReturnType() {
        return returnType;
    }

    @Override
    public Object processAdd(Object data, MinAggregatorState state) {
        if (data == null) {
            return state.currentValue();
        }
        return state.processAdd(data);
    }

    @Override
    public Object processAdd(Object[] data, MinAggregatorState state) {
        // will not occur
        return new IllegalStateException("Min cannot process data array, but found " + Arrays.deepToString(data));
    }

    @Override
    public Object processRemove(Object data, MinAggregatorState state) {
        if (data == null) {
            return state.currentValue();
        }
        return state.processRemove(data);
    }

    @Override
    public Object processRemove(Object[] data, MinAggregatorState state) {
        // will not occur
        return new IllegalStateException("Min cannot process data array, but found " + Arrays.deepToString(data));
    }

    @Override
    public Object reset(MinAggregatorState state) {
        return state.reset();
    }

    class MinAttributeAggregatorStateDouble extends MinAggregatorState {

        private final Attribute.Type type = Attribute.Type.DOUBLE;
        private Deque<Double> minDeque = null;
        private volatile Double minValue = null;

        public MinAttributeAggregatorStateDouble(boolean trackFutureStates) {
            if (trackFutureStates) {
                minDeque = new LinkedList<>();
            }
        }

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object processAdd(Object data) {
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
        public Object processRemove(Object data) {
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
        public Object reset() {
            if (minDeque != null) {
                minDeque.clear();
            }
            minValue = null;
            return null;
        }

        @Override
        public boolean canDestroy() {
            return (minDeque == null || minDeque.isEmpty()) && minValue == null;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("MinValue", minValue);
            state.put("MinDeque", minDeque);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            minValue = (Double) state.get("MinValue");
            minDeque = (Deque<Double>) state.get("MinDeque");
        }

        protected Object currentValue() {
            return minValue;
        }

    }

    class MinAttributeAggregatorStateFloat extends MinAggregatorState {

        private final Attribute.Type type = Attribute.Type.FLOAT;
        private Deque<Float> minDeque = null;
        private volatile Float minValue = null;

        public MinAttributeAggregatorStateFloat(boolean trackFutureStates) {
            if (trackFutureStates) {
                minDeque = new LinkedList<>();
            }
        }

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object processAdd(Object data) {
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
        public Object processRemove(Object data) {
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
        public Object reset() {
            if (minDeque != null) {
                minDeque.clear();
            }
            minValue = null;
            return null;
        }

        @Override
        public boolean canDestroy() {
            return (minDeque == null || minDeque.isEmpty()) && minValue == null;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("MinValue", minValue);
            state.put("MinDeque", minDeque);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            minValue = (Float) state.get("MinValue");
            minDeque = (Deque<Float>) state.get("MinDeque");
        }

        protected Object currentValue() {
            return minValue;
        }
    }

    class MinAttributeAggregatorStateInt extends MinAggregatorState {

        private final Attribute.Type type = Attribute.Type.INT;
        private Deque<Integer> minDeque = null;
        private volatile Integer minValue = null;

        public MinAttributeAggregatorStateInt(boolean trackFutureStates) {
            if (trackFutureStates) {
                minDeque = new LinkedList<>();
            }
        }

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object processAdd(Object data) {
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
        public Object reset() {
            if (minDeque != null) {
                minDeque.clear();
            }
            minValue = null;
            return null;
        }

        @Override
        public boolean canDestroy() {
            return (minDeque == null || minDeque.isEmpty()) && minValue == null;
        }

        @Override
        public Object processRemove(Object data) {
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
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("MinValue", minValue);
            state.put("MinDeque", minDeque);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            minValue = (Integer) state.get("MinValue");
            minDeque = (Deque<Integer>) state.get("MinDeque");
        }

        protected Object currentValue() {
            return minValue;
        }
    }

    class MinAttributeAggregatorStateLong extends MinAggregatorState {

        private final Attribute.Type type = Attribute.Type.LONG;
        private Deque<Long> minDeque = null;
        private volatile Long minValue = null;

        public MinAttributeAggregatorStateLong(boolean trackFutureStates) {
            if (trackFutureStates) {
                minDeque = new LinkedList<>();
            }
        }

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object processAdd(Object data) {
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
        public Object reset() {
            if (minDeque != null) {
                minDeque.clear();
            }
            minValue = null;
            return null;
        }

        @Override
        public boolean canDestroy() {
            return (minDeque == null || minDeque.isEmpty()) && minValue == null;
        }

        @Override
        public Object processRemove(Object data) {
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
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("MinValue", minValue);
            state.put("MinDeque", minDeque);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            minValue = (Long) state.get("MinValue");
            minDeque = (Deque<Long>) state.get("MinDeque");
        }

        protected Object currentValue() {
            return minValue;
        }

    }

    abstract class MinAggregatorState extends State {
        public abstract Object processAdd(Object data);

        public abstract Object processRemove(Object data);

        public abstract Object reset();

        protected abstract Object currentValue();
    }

}
