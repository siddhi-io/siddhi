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
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.executor.function.FunctionExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;

import java.util.LinkedList;
import java.util.List;

/**
 * Executor class for Siddhi event attribute that produces multiple values.
 * This executor is used to extract multiple attribute values as a list from
 * {@link ComplexEvent}.
 */
public class MultiValueVariableFunctionExecutor extends FunctionExecutor {

    int[] position;

    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
        position = ((VariableExpressionExecutor) attributeExpressionExecutors[0]).getPosition();
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
        List values = new LinkedList();
        StreamEvent aEvent = ((StateEvent) event).getStreamEvent(position);
        while (aEvent != null) {
            values.add(aEvent.getAttribute(position));
            aEvent = aEvent.getNext();
        }
        return values;
    }

}
