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

package io.siddhi.core.query.extension.util;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.selector.attribute.aggregator.AttributeAggregatorExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute.Type;

import java.util.HashMap;
import java.util.Map;

@Extension(
        name = "getAll",
        namespace = "custom",
        description = "Return the concatenations of the given input values.",
        parameters = {
                @Parameter(name = "args",
                        description = "The values that need to be concat.",
                        type = {DataType.STRING})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns the concatenated value as a string.",
                type = {DataType.STRING}),
        examples = @Example(
                syntax = "from inputStream\n" +
                        "select custom:getAll(\"hello\",\"_\",\"world\") as name\n" +
                        "insert into outputStream;",
                description = "This will concatenate given input values and return 'hello world'."
        )
)
public class StringConcatAggregatorExecutorString extends
        AttributeAggregatorExecutor<StringConcatAggregatorExecutorString.ExecutorState> {
    private static final long serialVersionUID = 1358667438272544590L;
    private boolean appendAbc = false;

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param processingMode               query processing mode
     * @param outputExpectsExpiredEvents   is expired events sent as output
     * @param configReader                 this hold the {@link StringConcatAggregatorExecutorString}
     *                                     configuration reader.
     * @param siddhiQueryContext           current siddhi query context
     */
    @Override
    protected StateFactory<ExecutorState> init(ExpressionExecutor[] attributeExpressionExecutors,
                                               ProcessingMode processingMode,
                                               boolean outputExpectsExpiredEvents, ConfigReader configReader,
                                               SiddhiQueryContext siddhiQueryContext) {
        appendAbc = Boolean.parseBoolean(configReader.readConfig("append.abc", "false"));
        return () -> new ExecutorState();
    }

    @Override
    public Type getReturnType() {
        return Type.STRING;
    }


    @Override
    public Object processAdd(Object data, ExecutorState state) {
        state.aggregatedStringValue = state.aggregatedStringValue + data;
        if (appendAbc) {
            return state.aggregatedStringValue + "-abc";
        } else {
            return state.aggregatedStringValue;
        }
    }

    @Override
    public Object processAdd(Object[] data, ExecutorState state) {
        for (Object aData : data) {
            state.aggregatedStringValue = state.aggregatedStringValue + aData;
        }
        if (appendAbc) {
            return state.aggregatedStringValue + "-abc";
        } else {
            return state.aggregatedStringValue;
        }
    }


    @Override
    public Object processRemove(Object data, ExecutorState state) {
        state.aggregatedStringValue = state.aggregatedStringValue.replaceFirst(data.toString(), "");
        if (appendAbc) {
            return state.aggregatedStringValue + "-abc";
        } else {
            return state.aggregatedStringValue;
        }
    }

    @Override
    public Object processRemove(Object[] data, ExecutorState state) {
        for (Object aData : data) {
            state.aggregatedStringValue = state.aggregatedStringValue.replaceFirst(aData.toString(), "");
        }
        if (appendAbc) {
            return state.aggregatedStringValue + "-abc";
        } else {
            return state.aggregatedStringValue;
        }
    }

    @Override
    public Object reset(ExecutorState state) {
        state.aggregatedStringValue = "";
        return state.aggregatedStringValue;
    }


    class ExecutorState extends State {
        private String aggregatedStringValue = "";

        @Override
        public boolean canDestroy() {
            return aggregatedStringValue != null && aggregatedStringValue.equals("");
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("AggregatedStringValue", aggregatedStringValue);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            aggregatedStringValue = (String) state.get("AggregatedStringValue");
        }
    }
}
