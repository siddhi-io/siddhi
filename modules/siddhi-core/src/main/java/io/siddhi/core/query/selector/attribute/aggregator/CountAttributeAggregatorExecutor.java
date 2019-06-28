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
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;

import java.util.HashMap;
import java.util.Map;

/**
 * {@link AttributeAggregatorExecutor} to calculate count.
 */
@Extension(
        name = "count",
        namespace = "",
        description = "Returns the count of all the events.",
        parameters = {
                @Parameter(name = "arg",
                        description = "This function accepts one parameter. " +
                                "It can belong to any one of the available types.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT,
                                DataType.STRING, DataType.BOOL, DataType.OBJECT},
                        dynamic = true,
                        optional = true)
        },
        parameterOverloads = {
                @ParameterOverload(),
                @ParameterOverload(parameterNames = {"arg"})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns the event count as a long.",
                type = {DataType.LONG}),
        examples = @Example(
                syntax = "from fooStream#window.timeBatch(10 sec)\n" +
                        "select count() as count\n" +
                        "insert into barStream;",
                description = "This will return the count of all the events for time batch in 10 seconds."
        )
)
public class CountAttributeAggregatorExecutor
        extends AttributeAggregatorExecutor<CountAttributeAggregatorExecutor.AggregatorState> {

    private static Attribute.Type type = Attribute.Type.LONG;

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param processingMode               query processing mode
     * @param outputExpectsExpiredEvents   is expired events sent as output
     * @param configReader                 this hold the {@link CountAttributeAggregatorExecutor} configuration reader.
     * @param siddhiQueryContext           Siddhi query runtime context
     */
    @Override
    protected StateFactory<AggregatorState> init(ExpressionExecutor[] attributeExpressionExecutors,
                                                 ProcessingMode processingMode,
                                                 boolean outputExpectsExpiredEvents,
                                                 ConfigReader configReader,
                                                 SiddhiQueryContext siddhiQueryContext) {
        return () -> new AggregatorState();

    }

    public Attribute.Type getReturnType() {
        return type;
    }

    @Override
    public Object processAdd(Object data, AggregatorState state) {
        state.count++;
        return state.count;
    }

    @Override
    public Object processAdd(Object[] data, AggregatorState state) {
        state.count++;
        return state.count;
    }

    @Override
    public Object processRemove(Object data, AggregatorState state) {
        state.count--;
        return state.count;
    }

    @Override
    public Object processRemove(Object[] data, AggregatorState state) {
        state.count--;
        return state.count;
    }

    @Override
    public Object reset(AggregatorState state) {
        state.count = 0L;
        return state.count;
    }


    class AggregatorState extends State {
        private long count = 0L;

        @Override
        public boolean canDestroy() {
            return count == 0L;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("Count", count);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            count = (long) state.get("Count");
        }
    }
}
