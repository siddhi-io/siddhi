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
import java.util.HashMap;
import java.util.Map;

/**
 * {@link AttributeAggregatorExecutor} to calculate max value for life time based on an event attribute.
 */
@Extension(
        name = "maxForever",
        namespace = "",
        description = "This is the attribute aggregator to store the maximum value for a given attribute throughout " +
                "the lifetime of the query regardless of any windows in-front.",
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
                syntax = "from inputStream\n" +
                        "select maxForever(temp) as max\n" +
                        "insert into outputStream;",
                description = "maxForever(temp) returns the maximum temp value recorded for all the events throughout" +
                        " the lifetime of the query."
        )
)
public class MaxForeverAttributeAggregatorExecutor
        extends AttributeAggregatorExecutor<MaxForeverAttributeAggregatorExecutor.MaxAggregatorState> {

    private Attribute.Type returnType;

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param processingMode               query processing mode
     * @param outputExpectsExpiredEvents   is expired events sent as output
     * @param configReader                 this hold the {@link MaxForeverAttributeAggregatorExecutor}
     *                                     configuration reader.
     * @param siddhiQueryContext           Siddhi query runtime context
     */
    @Override
    protected StateFactory<MaxAggregatorState> init(ExpressionExecutor[] attributeExpressionExecutors,
                                                    ProcessingMode processingMode,
                                                    boolean outputExpectsExpiredEvents, ConfigReader configReader,
                                                    SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new OperationNotSupportedException("MaxForever aggregator has to have exactly 1 parameter, " +
                    "currently " +
                    attributeExpressionExecutors.length + " parameters provided");
        }
        returnType = attributeExpressionExecutors[0].getReturnType();
        return new StateFactory<MaxAggregatorState>() {
            @Override
            public MaxAggregatorState createNewState() {
                switch (returnType) {
                    case FLOAT:
                        return new MaxForeverAttributeAggregatorStateFloat();
                    case INT:
                        return new MaxForeverAttributeAggregatorStateInt();
                    case LONG:
                        return new MaxForeverAttributeAggregatorStateLong();
                    case DOUBLE:
                        return new MaxForeverAttributeAggregatorStateDouble();
                    default:
                        throw new OperationNotSupportedException("MaxForever not supported for " + returnType);
                }
            }
        };

    }

    public Attribute.Type getReturnType() {
        return returnType;
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
        return new IllegalStateException("MaxForever cannot process data array, but found " +
                Arrays.deepToString(data));
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
        return new IllegalStateException("MaxForever cannot process data array, but found " +
                Arrays.deepToString(data));
    }


    @Override
    public Object reset(MaxAggregatorState state) {
        return state.reset();
    }

    class MaxForeverAttributeAggregatorStateDouble extends MaxAggregatorState {

        private volatile Double maxValue = null;

        @Override
        public Object processAdd(Object data) {
            Double value = (Double) data;
            if (maxValue == null || maxValue < value) {
                maxValue = value;
            }
            return maxValue;
        }

        @Override
        public Object processRemove(Object data) {
            Double value = (Double) data;
            if (maxValue == null || maxValue < value) {
                maxValue = value;
            }
            return maxValue;
        }

        @Override
        public Object reset() {
            return maxValue;
        }

        @Override
        public boolean canDestroy() {
            return maxValue == null;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("MaxValue", maxValue);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            maxValue = (Double) state.get("MaxValue");
        }

        protected Object currentValue() {
            return maxValue;
        }

    }

    class MaxForeverAttributeAggregatorStateFloat extends MaxAggregatorState {

        private volatile Float maxValue = null;

        @Override
        public Object processAdd(Object data) {
            Float value = (Float) data;
            if (maxValue == null || maxValue < value) {
                maxValue = value;
            }
            return maxValue;
        }

        @Override
        public Object processRemove(Object data) {
            Float value = (Float) data;
            if (maxValue == null || maxValue < value) {
                maxValue = value;
            }
            return maxValue;
        }

        @Override
        public Object reset() {
            return maxValue;
        }

        @Override
        public boolean canDestroy() {
            return maxValue == null;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("MaxValue", maxValue);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            maxValue = (Float) state.get("MaxValue");
        }

        protected Object currentValue() {
            return maxValue;
        }

    }

    class MaxForeverAttributeAggregatorStateInt extends MaxAggregatorState {

        private volatile Integer maxValue = null;

        @Override
        public Object processAdd(Object data) {
            Integer value = (Integer) data;
            if (maxValue == null || maxValue < value) {
                maxValue = value;
            }
            return maxValue;
        }

        @Override
        public Object processRemove(Object data) {
            Integer value = (Integer) data;
            if (maxValue == null || maxValue < value) {
                maxValue = value;
            }
            return maxValue;
        }

        @Override
        public Object reset() {
            return maxValue;
        }

        @Override
        public boolean canDestroy() {
            return maxValue == null;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("MaxValue", maxValue);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            maxValue = (Integer) state.get("MaxValue");
        }

        protected Object currentValue() {
            return maxValue;
        }
    }

    class MaxForeverAttributeAggregatorStateLong extends MaxAggregatorState {

        private volatile Long maxValue = null;

        @Override
        public Object processAdd(Object data) {
            Long value = (Long) data;
            if (maxValue == null || maxValue < value) {
                maxValue = value;
            }
            return maxValue;
        }

        @Override
        public Object processRemove(Object data) {
            Long value = (Long) data;
            if (maxValue == null || maxValue < value) {
                maxValue = value;
            }
            return maxValue;
        }

        @Override
        public Object reset() {
            return maxValue;
        }

        @Override
        public boolean canDestroy() {
            return maxValue == null;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("MaxValue", maxValue);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            maxValue = (Long) state.get("MaxValue");
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
