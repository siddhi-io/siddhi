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
 * {@link AttributeAggregatorExecutor} to calculate average based on an event attribute.
 */
@Extension(
        name = "avg",
        namespace = "",
        description = "Calculates the average for all the events.",
        parameters = {
                @Parameter(name = "arg",
                        description = "The value that need to be averaged.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT},
                        dynamic = true)
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"arg"})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns the calculated average value as a double.",
                type = {DataType.DOUBLE}),
        examples = @Example(
                syntax = "from fooStream#window.timeBatch\n select avg(temp) as avgTemp\n insert into barStream;",
                description = "avg(temp) returns the average temp value for all the events based on their " +
                        "arrival and expiry."
        )
)
public class AvgAttributeAggregatorExecutor
        extends AttributeAggregatorExecutor<AvgAttributeAggregatorExecutor.AvgAttributeState> {

    private Attribute.Type returnType;

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param processingMode               query processing mode
     * @param outputExpectsExpiredEvents   is expired events sent as output
     * @param configReader                 this hold the {@link AvgAttributeAggregatorExecutor} configuration reader.
     * @param siddhiQueryContext           Siddhi query runtime context
     */
    @Override
    protected StateFactory<AvgAttributeState> init(ExpressionExecutor[] attributeExpressionExecutors,
                                                   ProcessingMode processingMode,
                                                   boolean outputExpectsExpiredEvents, ConfigReader configReader,
                                                   SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new OperationNotSupportedException("Avg aggregator has to have exactly 1 parameter, currently " +
                    attributeExpressionExecutors.length + " parameters provided");
        }
        returnType = Attribute.Type.DOUBLE;
        Attribute.Type type = attributeExpressionExecutors[0].getReturnType();
        return () -> {
            switch (type) {
                case FLOAT:
                    return new AvgAttributeAggregatorStateFloat();
                case INT:
                    return new AvgAttributeAggregatorStateInt();
                case LONG:
                    return new AvgAttributeAggregatorStateLong();
                case DOUBLE:
                    return new AvgAttributeAggregatorStateDouble();
                default:
                    throw new OperationNotSupportedException("Avg not supported for " + returnType);
            }
        };

    }

    public Attribute.Type getReturnType() {
        return returnType;
    }

    @Override
    public Object processAdd(Object data, AvgAttributeState state) {
        if (data == null) {
            return state.currentValue();
        }
        return state.processAdd(data);
    }

    @Override
    public Object processAdd(Object[] data, AvgAttributeState state) {
        // will not occur
        return new IllegalStateException("Avg cannot process data array, but found " + Arrays.deepToString(data));
    }

    @Override
    public Object processRemove(Object data, AvgAttributeState state) {
        if (data == null) {
            return state.currentValue();
        }
        return state.processRemove(data);
    }

    @Override
    public Object processRemove(Object[] data, AvgAttributeState state) {
        // will not occur
        return new IllegalStateException("Avg cannot process data array, but found " + Arrays.deepToString(data));
    }

    @Override
    public Object reset(AvgAttributeState state) {
        return state.reset();
    }

    class AvgAttributeAggregatorStateDouble extends AvgAttributeState {

        private double value = 0.0;
        private long count = 0;

        @Override
        public Object processAdd(Object data) {
            count++;
            value += (Double) data;
            if (count == 0) {
                return null;
            }
            return value / count;
        }

        @Override
        public Object processRemove(Object obj) {
            count--;
            value -= (Double) obj;
            if (count == 0) {
                return null;
            }
            return value / count;
        }

        @Override
        public Object reset() {
            value = 0.0;
            count = 0;
            return null;
        }

        @Override
        public boolean canDestroy() {
            return value == 0.0 && count == 0;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("Value", value);
            state.put("Count", count);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            value = (double) state.get("Value");
            count = (long) state.get("Count");
        }

        protected Object currentValue() {
            if (count == 0) {
                return null;
            }
            return value / count;
        }
    }

    class AvgAttributeAggregatorStateFloat extends AvgAttributeState {

        private double value = 0.0;
        private long count = 0;

        @Override
        public Object processAdd(Object data) {
            count++;
            value += (Float) data;
            if (count == 0) {
                return null;
            }
            return value / count;
        }

        @Override
        public Object processRemove(Object obj) {
            count--;
            value -= (Float) obj;
            if (count == 0) {
                return null;
            }
            return value / count;
        }

        @Override
        public Object reset() {
            value = 0.0;
            count = 0;
            return null;
        }

        @Override
        public boolean canDestroy() {
            return value == 0.0 && count == 0;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("Value", value);
            state.put("Count", count);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            value = (double) state.get("Value");
            count = (long) state.get("Count");
        }

        protected Object currentValue() {
            if (count == 0) {
                return null;
            }
            return value / count;
        }
    }

    class AvgAttributeAggregatorStateInt extends AvgAttributeState {

        private double value = 0.0;
        private long count = 0;

        @Override
        public Object processAdd(Object data) {
            count++;
            value += (Integer) data;
            if (count == 0) {
                return null;
            }
            return value / count;
        }

        @Override
        public Object processRemove(Object obj) {
            count--;
            value -= (Integer) obj;
            if (count == 0) {
                return null;
            }
            return value / count;
        }

        @Override
        public Object reset() {
            value = 0.0;
            count = 0;
            return null;
        }

        @Override
        public boolean canDestroy() {
            return value == 0.0 && count == 0;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("Value", value);
            state.put("Count", count);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            value = (double) state.get("Value");
            count = (long) state.get("Count");
        }

        protected Object currentValue() {
            if (count == 0) {
                return null;
            }
            return value / count;
        }
    }

    class AvgAttributeAggregatorStateLong extends AvgAttributeState {

        private double value = 0.0;
        private long count = 0;

        @Override
        public Object processAdd(Object data) {
            count++;
            value += (Long) data;
            if (count == 0) {
                return null;
            }
            return value / count;
        }

        @Override
        public Object processRemove(Object obj) {
            count--;
            value -= (Long) obj;
            if (count == 0) {
                return null;
            }
            return value / count;
        }

        @Override
        public Object reset() {
            value = 0.0;
            count = 0;
            return null;
        }

        @Override
        public boolean canDestroy() {
            return value == 0.0 && count == 0;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("Value", value);
            state.put("Count", count);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            value = (double) state.get("Value");
            count = (long) state.get("Count");
        }

        protected Object currentValue() {
            if (count == 0) {
                return null;
            }
            return value / count;
        }
    }

    abstract class AvgAttributeState extends State {
        public abstract Object processAdd(Object data);

        public abstract Object processRemove(Object obj);

        public abstract Object reset();

        protected abstract Object currentValue();
    }


}
