/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import java.util.HashMap;
import java.util.Map;

/**
 * {@link AttributeAggregatorExecutor} to calculate sum based on an event attribute.
 */
@Extension(
        name = "or",
        namespace = "",
        description = "Returns the results of OR operation for all the events.",
        parameters = {
                @Parameter(name = "arg",
                        description = "The value that needs to be OR operation.",
                        type = {DataType.BOOL},
                        dynamic = true)
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"arg"})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns false only if all of its operands are false, else true.",
                type = {DataType.BOOL}),
        examples = {
                @Example(
                        syntax = "from cscStream#window.lengthBatch(10)\n" +
                                "select or(isFraud) as isFraudTransaction\n" +
                                "insert into alertStream;",
                        description = "This will returns the result for OR operation of isFraud values as a " +
                                "boolean value for event chunk expiry by window length batch."
                )
        }
)
public class OrAttributeAggregatorExecutor extends
        AttributeAggregatorExecutor<OrAttributeAggregatorExecutor.AggregatorState> {

    private static Attribute.Type type = Attribute.Type.BOOL;

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param processingMode               query processing mode
     * @param outputExpectsExpiredEvents   is expired events sent as output
     * @param configReader                 this hold the {@link OrAttributeAggregatorExecutor} configuration reader.
     * @param siddhiQueryContext           Siddhi query runtime context
     */
    @Override
    protected StateFactory<AggregatorState> init(ExpressionExecutor[] attributeExpressionExecutors,
                                                 ProcessingMode processingMode,
                                                 boolean outputExpectsExpiredEvents, ConfigReader configReader,
                                                 SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new OperationNotSupportedException("And aggregator has to have exactly 1 parameter, currently " +
                    attributeExpressionExecutors.length
                    + " parameters provided");
        }
        return () -> new AggregatorState();
    }

    public Attribute.Type getReturnType() {
        return type;
    }

    @Override
    public Object processAdd(Object data, AggregatorState state) {
        if ((boolean) data) {
            state.trueEventsCount++;
        }
        return computeLogicalOperation(state);
    }

    @Override
    public Object processAdd(Object[] data, AggregatorState state) {
        for (Object object : data) {
            if ((boolean) object) {
                state.trueEventsCount++;
            }
        }
        return computeLogicalOperation(state);
    }

    @Override
    public Object processRemove(Object data, AggregatorState state) {
        if ((boolean) data) {
            state.trueEventsCount--;
        }
        return computeLogicalOperation(state);
    }

    @Override
    public Object processRemove(Object[] data, AggregatorState state) {
        for (Object object : data) {
            if ((boolean) object) {
                state.trueEventsCount--;
            }
        }
        return computeLogicalOperation(state);
    }

    @Override
    public Object reset(AggregatorState state) {
        state.trueEventsCount = 0;
        return false;
    }

    private boolean computeLogicalOperation(AggregatorState state) {
        return state.trueEventsCount > 0;
    }

    class AggregatorState extends State {
        private int trueEventsCount = 0;

        @Override
        public boolean canDestroy() {
            return trueEventsCount == 0;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("TrueEventsCount", trueEventsCount);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            trueEventsCount = (int) state.get("TrueEventsCount");
        }
    }
}
