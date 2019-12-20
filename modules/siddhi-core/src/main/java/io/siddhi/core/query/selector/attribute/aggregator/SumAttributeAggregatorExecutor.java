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
 * {@link AttributeAggregatorExecutor} to calculate sum based on an event attribute.
 */
@Extension(
        name = "sum",
        namespace = "",
        description = "Returns the sum for all the events.",
        parameters = {
                @Parameter(name = "arg",
                        description = "The value that needs to be summed.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT},
                        dynamic = true)
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"arg"})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns long if the input parameter type is int or long, and returns double if the " +
                        "input parameter type is float or double.",
                type = {DataType.LONG, DataType.DOUBLE}),
        examples = {
                @Example(
                        syntax = "from inputStream\n" +
                                "select sum(volume) as sumOfVolume\n" +
                                "insert into outputStream;",
                        description = "This will returns the sum of volume values as a long value for each event " +
                                "arrival and expiry."
                )
        }
)
public class SumAttributeAggregatorExecutor
        extends AttributeAggregatorExecutor<SumAttributeAggregatorExecutor.AggregatorState> {

    private Attribute.Type returnType;

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param processingMode               query processing mode
     * @param outputExpectsExpiredEvents   is expired events sent as output
     * @param configReader                 this hold the {@link SumAttributeAggregatorExecutor} configuration reader.
     * @param siddhiQueryContext           Siddhi query runtime context
     */
    @Override
    protected StateFactory<AggregatorState> init(ExpressionExecutor[] attributeExpressionExecutors,
                                                 ProcessingMode processingMode,
                                                 boolean outputExpectsExpiredEvents, ConfigReader configReader,
                                                 SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new OperationNotSupportedException("Sum aggregator has to have exactly 1 parameter, currently " +
                    attributeExpressionExecutors.length
                    + " parameters provided");
        }
        Attribute.Type type = attributeExpressionExecutors[0].getReturnType();
        switch (type) {
            case INT:
            case LONG:
                returnType = Attribute.Type.LONG;
                break;
            case FLOAT:
            case DOUBLE:
                returnType = Attribute.Type.DOUBLE;
                break;
            default:
                throw new OperationNotSupportedException("Sum not supported for " + returnType);
        }
        return new StateFactory<AggregatorState>() {
            @Override
            public AggregatorState createNewState() {
                switch (type) {
                    case FLOAT:
                        return new AggregatorStateFloat();
                    case INT:
                        return new AggregatorStateInt();
                    case LONG:
                        return new AggregatorStateLong();
                    case DOUBLE:
                        return new AggregatorStateDouble();
                    default:
                        throw new OperationNotSupportedException("Sum not supported for " + returnType);
                }
            }
        };


    }

    public Attribute.Type getReturnType() {
        return returnType;
    }

    @Override
    public Object processAdd(Object data, AggregatorState state) {
        if (data == null) {
            return state.currentValue();
        }
        return state.processAdd(data);
    }

    @Override
    public Object processAdd(Object[] data, AggregatorState state) {
        // will not occur
        return new IllegalStateException("Sin cannot process data array, but found " + Arrays.deepToString(data));
    }

    @Override
    public Object processRemove(Object data, AggregatorState state) {
        if (data == null) {
            return state.currentValue();
        }
        return state.processRemove(data);
    }

    @Override
    public Object processRemove(Object[] data, AggregatorState state) {
        // will not occur
        return new IllegalStateException("Sin cannot process data array, but found " + Arrays.deepToString(data));
    }

    @Override
    public Object reset(AggregatorState state) {
        return state.reset();
    }

    class AggregatorStateDouble extends AggregatorState {

        private final Attribute.Type type = Attribute.Type.DOUBLE;
        private double sum = 0.0;
        private long count = 0;

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object processAdd(Object data) {
            return processAdd(((Double) data).doubleValue());
        }

        @Override
        public Object processRemove(Object data) {
            return processRemove(((Double) data).doubleValue());
        }

        public Object processAdd(double data) {
            sum += data;
            count++;
            return sum;
        }

        public Object processRemove(double data) {
            sum -= data;
            count--;
            if (count == 0) {
                return null;
            } else {
                return sum;
            }
        }

        @Override
        public Object reset() {
            sum = 0.0;
            count = 0;
            return null;
        }

        @Override
        public boolean canDestroy() {
            return count == 0 && sum == 0.0;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("Sum", sum);
            state.put("Count", count);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            sum = (double) state.get("Sum");
            count = (long) state.get("Count");
        }

        protected Object currentValue() {
            if (count == 0) {
                return null;
            } else {
                return sum;
            }
        }

    }

    class AggregatorStateFloat extends AggregatorStateDouble {

        @Override
        public Object processAdd(Object data) {
            if (data == null) {
                return null;
            }
            return processAdd(((Float) data).doubleValue());
        }

        @Override
        public Object processRemove(Object data) {
            if (data == null) {
                return null;
            }
            return processRemove(((Float) data).doubleValue());
        }

    }

    class AggregatorStateLong extends AggregatorState {

        private final Attribute.Type type = Attribute.Type.LONG;
        private long sum = 0L;
        private long count = 0;

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object processAdd(Object data) {
            return processAdd(((Long) data).longValue());
        }

        public Object processAdd(long data) {
            sum += data;
            count++;
            return sum;
        }


        @Override
        public Object processRemove(Object data) {
            return processRemove(((Long) data).longValue());
        }

        public Object processRemove(double data) {
            sum -= data;
            count--;
            if (count == 0) {
                return null;
            } else {
                return sum;

            }
        }

        public Object reset() {
            sum = 0L;
            count = 0;
            return sum;
        }

        @Override
        public boolean canDestroy() {
            return count == 0 && sum == 0L;
        }


        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("Sum", sum);
            state.put("Count", count);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            sum = (long) state.get("Sum");
            count = (long) state.get("Count");
        }

        protected Object currentValue() {
            if (count == 0) {
                return null;
            } else {
                return sum;
            }
        }

    }

    class AggregatorStateInt extends AggregatorStateLong {

        @Override
        public Object processAdd(Object data) {
            return processAdd(((Integer) data).longValue());
        }

        @Override
        public Object processRemove(Object data) {
            return processRemove(((Integer) data).longValue());
        }

    }

    abstract class AggregatorState extends State {

        protected abstract Object currentValue();

        public abstract Object processAdd(Object data);

        public abstract Object processRemove(Object data);

        public abstract Object reset();

    }
}
