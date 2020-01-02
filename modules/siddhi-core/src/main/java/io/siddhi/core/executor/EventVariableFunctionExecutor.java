/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.core.executor;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.executor.function.FunctionExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;

import static io.siddhi.core.util.SiddhiConstants.STREAM_EVENT_CHAIN_INDEX;
import static io.siddhi.core.util.SiddhiConstants.STREAM_EVENT_INDEX_IN_CHAIN;
import static io.siddhi.core.util.SiddhiConstants.UNKNOWN_STATE;

/**
 * Executor class for Siddhi that extracts events.
 */
public class EventVariableFunctionExecutor extends FunctionExecutor {

    private int[] position = new int[]{UNKNOWN_STATE, UNKNOWN_STATE};

    public EventVariableFunctionExecutor(int streamEventChainIndex, int streamEventIndexInChain) {
        position[STREAM_EVENT_CHAIN_INDEX] = streamEventChainIndex;
        position[STREAM_EVENT_INDEX_IN_CHAIN] = streamEventIndexInChain;
    }

    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
        return null;
    }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.OBJECT;
    }

    @Override
    protected Object execute(Object[] data, State state) {
        //will not occur
        return null;
    }

    @Override
    protected Object execute(Object data, State state) {
        //will not occur
        return null;
    }

    public Object execute(ComplexEvent event) {
        return ((StateEvent) event).getStreamEvent(position);
    }

}
