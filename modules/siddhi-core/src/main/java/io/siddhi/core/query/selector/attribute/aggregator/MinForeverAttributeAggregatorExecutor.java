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
 * {@link AttributeAggregatorExecutor} to calculate min value for life time based on an event attribute.
 */
@Extension(
        name = "minForever",
        namespace = "",
        description = "This is the attribute aggregator to store the minimum value for a given attribute throughout " +
                "the lifetime of the query regardless of any windows in-front.",
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
                description = "Returns the minimum value in the same data type as the input.",
                type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT}),
        examples = @Example(
                syntax = "from inputStream\n" +
                        "select minForever(temp) as max\n" +
                        "insert into outputStream;",
                description = "minForever(temp) returns the minimum temp value recorded for all the events throughout" +
                        "the lifetime of the query."
        )
)
public class MinForeverAttributeAggregatorExecutor
        extends AttributeAggregatorExecutor<MinForeverAttributeAggregatorExecutor.MinAggregatorState> {

    private Attribute.Type returnType;

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param processingMode               query processing mode
     * @param outputExpectsExpiredEvents   is expired events sent as output
     * @param configReader                 this hold the {@link MinForeverAttributeAggregatorExecutor}
     *                                     configuration reader.
     * @param siddhiQueryContext           Siddhi query runtime context
     */
    @Override
    protected StateFactory<MinAggregatorState> init(ExpressionExecutor[] attributeExpressionExecutors,
                                                    ProcessingMode processingMode,
                                                    boolean outputExpectsExpiredEvents, ConfigReader configReader,
                                                    SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new OperationNotSupportedException("MinForever aggregator has to have exactly 1 parameter, " +
                    "currently " + attributeExpressionExecutors.length + " parameters provided");
        }
        returnType = attributeExpressionExecutors[0].getReturnType();
        return new StateFactory<MinAggregatorState>() {
            @Override
            public MinAggregatorState createNewState() {
                switch (returnType) {
                    case FLOAT:
                        return new MinForeverAttributeAggregatorStateFloat();
                    case INT:
                        return new MinForeverAttributeAggregatorStateInt();
                    case LONG:
                        return new MinForeverAttributeAggregatorStateLong();
                    case DOUBLE:
                        return new MinForeverAttributeAggregatorStateDouble();
                    default:
                        throw new OperationNotSupportedException("MinForever not supported for " + returnType);
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
        return new IllegalStateException("MinForever cannot process data array, but found " +
                Arrays.deepToString(data));
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
        return new IllegalStateException("MinForever cannot process data array, but found " +
                Arrays.deepToString(data));
    }

    @Override
    public Object reset(MinAggregatorState state) {
        return state.reset();
    }

    class MinForeverAttributeAggregatorStateDouble extends MinAggregatorState {

        private final Attribute.Type type = Attribute.Type.DOUBLE;
        private volatile Double minValue = null;

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object processAdd(Object data) {
            Double value = (Double) data;
            if (minValue == null || minValue > value) {
                minValue = value;
            }
            return minValue;
        }

        @Override
        public Object processRemove(Object data) {
            Double value = (Double) data;
            if (minValue == null || minValue > value) {
                minValue = value;
            }
            return minValue;
        }

        @Override
        public Object reset() {
            return minValue;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("MinValue", minValue);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            minValue = (Double) state.get("MinValue");
        }

        protected Object currentValue() {
            return minValue;
        }

        @Override
        public boolean canDestroy() {
            return minValue == null;
        }

    }

    class MinForeverAttributeAggregatorStateFloat extends MinAggregatorState {

        private final Attribute.Type type = Attribute.Type.FLOAT;
        private volatile Float minValue = null;

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object processAdd(Object data) {
            Float value = (Float) data;
            if (minValue == null || minValue > value) {
                minValue = value;
            }
            return minValue;
        }

        @Override
        public Object processRemove(Object data) {
            Float value = (Float) data;
            if (minValue == null || minValue > value) {
                minValue = value;
            }
            return minValue;
        }

        @Override
        public Object reset() {
            return minValue;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("MinValue", minValue);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            minValue = (Float) state.get("MinValue");
        }

        protected Object currentValue() {
            return minValue;
        }

        @Override
        public boolean canDestroy() {
            return minValue == null;
        }
    }

    class MinForeverAttributeAggregatorStateInt extends MinAggregatorState {

        private final Attribute.Type type = Attribute.Type.INT;
        private volatile Integer minValue = null;

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object processAdd(Object data) {
            Integer value = (Integer) data;
            if (minValue == null || minValue > value) {
                minValue = value;
            }
            return minValue;
        }

        @Override
        public Object reset() {
            return minValue;
        }

        @Override
        public Object processRemove(Object data) {
            Integer value = (Integer) data;
            if (minValue == null || minValue > value) {
                minValue = value;
            }
            return minValue;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("MinValue", minValue);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            minValue = (Integer) state.get("MinValue");
        }

        protected Object currentValue() {
            return minValue;
        }

        @Override
        public boolean canDestroy() {
            return minValue == null;
        }
    }

    class MinForeverAttributeAggregatorStateLong extends MinAggregatorState {

        private final Attribute.Type type = Attribute.Type.LONG;
        private volatile Long minValue = null;

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object processAdd(Object data) {
            Long value = (Long) data;
            if (minValue == null || minValue > value) {
                minValue = value;
            }
            return minValue;
        }

        @Override
        public Object reset() {
            return minValue;
        }

        @Override
        public Object processRemove(Object data) {
            Long value = (Long) data;
            if (minValue == null || minValue > value) {
                minValue = value;
            }
            return minValue;
        }

        @Override
        public boolean canDestroy() {
            return minValue == null;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("MinValue", minValue);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            minValue = (Long) state.get("MinValue");
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
