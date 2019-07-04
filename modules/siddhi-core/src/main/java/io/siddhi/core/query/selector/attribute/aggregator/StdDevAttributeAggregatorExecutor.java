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
 * {@link AttributeAggregatorExecutor} to calculate standard deviation based on an event attribute.
 */
@Extension(
        name = "stdDev",
        namespace = "",
        description = "Returns the calculated standard deviation for all the events.",
        parameters = {
                @Parameter(name = "arg",
                        description = "The value that should be used to calculate the standard deviation.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT},
                        dynamic = true)
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"arg"})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns the calculated standard deviation value as a double.",
                type = {DataType.DOUBLE}),
        examples = @Example(
                syntax = "from inputStream\n" +
                        "select stddev(temp) as stdTemp\n" +
                        "insert into outputStream;",
                description = "stddev(temp) returns the calculated standard deviation of temp for all the events " +
                        "based on their arrival and expiry."
        )
)
public class StdDevAttributeAggregatorExecutor
        extends AttributeAggregatorExecutor<StdDevAttributeAggregatorExecutor.AggregatorState> {
    private Attribute.Type returnType;

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param processingMode               query processing mode
     * @param outputExpectsExpiredEvents   is expired events sent as output
     * @param configReader                 this hold the {@link StdDevAttributeAggregatorExecutor} configuration reader.
     * @param siddhiQueryContext           Siddhi query runtime context
     */
    @Override
    protected StateFactory<AggregatorState> init(ExpressionExecutor[] attributeExpressionExecutors,
                                                 ProcessingMode processingMode,
                                                 boolean outputExpectsExpiredEvents, ConfigReader configReader,
                                                 SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new OperationNotSupportedException("stdDev aggregator has to have exactly 1 parameter, currently " +
                    attributeExpressionExecutors.length
                    + " parameters provided");
        }
        returnType = Attribute.Type.DOUBLE;
        Attribute.Type type = attributeExpressionExecutors[0].getReturnType();
        return () -> {
            switch (type) {
                case INT:
                    return new StdDevAttributeAggregatorStateInt();
                case LONG:
                    return new StdDevAttributeAggregatorStateLong();
                case FLOAT:
                    return new StdDevAttributeAggregatorStateFloat();
                case DOUBLE:
                    return new StdDevAttributeAggregatorStateDouble();
                default:
                    throw new OperationNotSupportedException("stdDev not supported for " + returnType);
            }
        };

    }

    @Override
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
        return new IllegalStateException("stdDev cannot process data array, but found " + Arrays.deepToString(data));
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
        return new IllegalStateException("stdDev cannot process data array, but found " + Arrays.deepToString(data));
    }

    @Override
    public Object reset(AggregatorState state) {
        return state.reset();
    }

    /**
     * Standard deviation abstrct aggregator for Double values
     */
    abstract class AggregatorState extends State {

        private double mean, stdDeviation, sum;
        private int count = 0;

        public abstract Object processRemove(Object data);

        public abstract Object processAdd(Object data);

        public Object processAdd(double value) {
            // See here for the algorithm: http://www.johndcook.com/blog/standard_deviation/
            count++;
            if (count == 0) {
                return null;
            } else if (count == 1) {
                sum = mean = value;
                stdDeviation = 0.0;
                return 0.0;
            } else {
                double oldMean = mean;
                sum += value;
                mean = sum / count;
                stdDeviation += (value - oldMean) * (value - mean);
                return Math.sqrt(stdDeviation / count);
            }
        }

        public Object processRemove(double value) {
            count--;
            if (count == 0) {
                sum = mean = 0.0;
                stdDeviation = 0.0;
                return null;
            } else {
                double oldMean = mean;
                sum -= value;
                mean = sum / count;
                stdDeviation -= (value - oldMean) * (value - mean);
            }

            if (count == 1) {
                return 0.0;
            }
            return Math.sqrt(stdDeviation / count);
        }

        public Object reset() {
            sum = mean = 0.0;
            stdDeviation = 0.0;
            count = 0;
            return null;
        }

        @Override
        public boolean canDestroy() {
            return count == 0 && sum == 0.0 && mean == 0.0 && stdDeviation == 0.0;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("Sum", sum);
            state.put("Mean", mean);
            state.put("stdDeviation", stdDeviation);
            state.put("Count", count);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            sum = (Long) state.get("Sum");
            mean = (Long) state.get("Mean");
            stdDeviation = (Long) state.get("stdDeviation");
            count = (int) state.get("Count");
        }

        protected Object currentValue() {
            if (count == 0) {
                return null;
            } else if (count == 1) {
                return 0.0;
            }
            return Math.sqrt(stdDeviation / count);
        }
    }

    /**
     * Standard deviation aggregator for Double values
     */
    private class StdDevAttributeAggregatorStateDouble extends AggregatorState {
        @Override
        public Object processAdd(Object data) {
            return processAdd(((Double) data).doubleValue());
        }

        @Override
        public Object processRemove(Object data) {
            return processRemove(((Double) data).doubleValue());
        }
    }

    /**
     * Standard deviation aggregator for Float values
     */
    private class StdDevAttributeAggregatorStateFloat extends AggregatorState {
        @Override
        public Object processAdd(Object data) {
            return processAdd(((Float) data).doubleValue());
        }

        @Override
        public Object processRemove(Object data) {
            return processRemove(((Float) data).doubleValue());
        }
    }

    /**
     * Standard deviation aggregator for Integer values
     */
    private class StdDevAttributeAggregatorStateInt extends AggregatorState {
        @Override
        public Object processAdd(Object data) {
            return processAdd(((Integer) data).doubleValue());
        }

        @Override
        public Object processRemove(Object data) {
            return processRemove(((Integer) data).doubleValue());
        }
    }

    /**
     * Standard deviation aggregator for Long values
     */
    private class StdDevAttributeAggregatorStateLong extends AggregatorState {
        @Override
        public Object processAdd(Object data) {
            return processAdd(((Long) data).doubleValue());
        }

        @Override
        public Object processRemove(Object data) {
            return processRemove(((Long) data).doubleValue());
        }
    }
}
