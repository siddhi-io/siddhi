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
package org.wso2.siddhi.core.query.selector.attribute.aggregator;

import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Extension(
        name = "sum",
        namespace = "",
        description = "Returns the sum for all the events.",
        parameters = {
                @Parameter(name = "arg",
                        description = "The value that needs to be summed.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT})
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
public class SumAttributeAggregator extends AttributeAggregator {

    private SumAttributeAggregator sumOutputAttributeAggregator;

    /**
     * The initialization method for FunctionExecutor
     *  @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param configReader
     * @param executionPlanContext         Execution plan runtime context
     */
    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new OperationNotSupportedException("Sum aggregator has to have exactly 1 parameter, currently " +
                    attributeExpressionExecutors.length + " parameters provided");
        }
        Attribute.Type type = attributeExpressionExecutors[0].getReturnType();
        switch (type) {
            case FLOAT:
                sumOutputAttributeAggregator = new SumAttributeAggregatorFloat();
                break;
            case INT:
                sumOutputAttributeAggregator = new SumAttributeAggregatorInt();
                break;
            case LONG:
                sumOutputAttributeAggregator = new SumAttributeAggregatorLong();
                break;
            case DOUBLE:
                sumOutputAttributeAggregator = new SumAttributeAggregatorDouble();
                break;
            default:
                throw new OperationNotSupportedException("Sum not supported for " + type);
        }

    }

    public Attribute.Type getReturnType() {
        return sumOutputAttributeAggregator.getReturnType();
    }

    @Override
    public Object processAdd(Object data) {
        return sumOutputAttributeAggregator.processAdd(data);
    }

    @Override
    public Object processAdd(Object[] data) {
        // will not occur
        return new IllegalStateException("Sin cannot process data array, but found " + Arrays.deepToString(data));
    }

    @Override
    public Object processRemove(Object data) {
        return sumOutputAttributeAggregator.processRemove(data);
    }

    @Override
    public Object processRemove(Object[] data) {
        // will not occur
        return new IllegalStateException("Sin cannot process data array, but found " + Arrays.deepToString(data));
    }

    @Override
    public Object reset() {
        return sumOutputAttributeAggregator.reset();
    }

    @Override
    public void start() {
        //Nothing to start
    }

    @Override
    public void stop() {
        //Nothing to stop
    }

    @Override
    public Map<String, Object> currentState() {
        return sumOutputAttributeAggregator.currentState();
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        sumOutputAttributeAggregator.restoreState(state);
    }

    class SumAttributeAggregatorDouble extends SumAttributeAggregator {

        private final Attribute.Type type = Attribute.Type.DOUBLE;
        private double sum = 0.0;

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object processAdd(Object data) {
            sum += (Double) data;
            return sum;
        }

        @Override
        public Object processRemove(Object data) {
            sum -= (Double) data;
            return sum;
        }

        @Override
        public Object reset() {
            sum = 0.0;
            return sum;
        }

        @Override
        public Map<String, Object> currentState() {
            Map<String, Object> state = new HashMap<>();
            state.put("Sum", sum);
            return state;
        }

        @Override
        public void restoreState(Map<String, Object> state) {
            sum = (double) state.get("Sum");
        }

    }

    class SumAttributeAggregatorFloat extends SumAttributeAggregator {

        private final Attribute.Type type = Attribute.Type.DOUBLE;
        private double sum = 0.0;

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object processAdd(Object data) {
            sum += ((Float) data).doubleValue();
            return sum;
        }

        @Override
        public Object processRemove(Object data) {
            sum -= ((Float) data).doubleValue();
            return sum;
        }

        public Object reset() {
            sum = 0.0;
            return sum;
        }

        @Override
        public Map<String, Object> currentState() {
            Map<String, Object> state = new HashMap<>();
            state.put("Sum", sum);
            return state;
        }

        @Override
        public void restoreState(Map<String, Object> state) {
            sum = (double) state.get("Sum");
        }

    }

    class SumAttributeAggregatorInt extends SumAttributeAggregator {

        private final Attribute.Type type = Attribute.Type.LONG;
        private long sum = 0L;

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object processAdd(Object data) {
            sum += ((Integer) data).longValue();
            return sum;
        }

        @Override
        public Object processRemove(Object data) {
            sum -= ((Integer) data).longValue();
            return sum;
        }

        public Object reset() {
            sum = 0L;
            return sum;
        }

        @Override
        public Map<String, Object> currentState() {
            Map<String, Object> state = new HashMap<>();
            state.put("Sum", sum);
            return state;
        }

        @Override
        public void restoreState(Map<String, Object> state) {
            sum = (long) state.get("Sum");
        }

    }

    class SumAttributeAggregatorLong extends SumAttributeAggregator {

        private final Attribute.Type type = Attribute.Type.LONG;
        private long sum = 0L;

        public Attribute.Type getReturnType() {
            return type;
        }

        @Override
        public Object processAdd(Object data) {
            sum += (Long) data;
            return sum;
        }

        @Override
        public Object processRemove(Object data) {
            sum -= (Long) data;
            return sum;
        }

        public Object reset() {
            sum = 0L;
            return sum;
        }

        @Override
        public Map<String, Object> currentState() {
            Map<String, Object> state = new HashMap<>();
            state.put("Sum", sum);
            return state;
        }

        @Override
        public void restoreState(Map<String, Object> state) {
            sum = (long) state.get("Sum");
        }

    }

}
