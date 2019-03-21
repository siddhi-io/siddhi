/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.core.executor.incremental;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.exception.OperationNotSupportedException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.function.FunctionExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.HashMap;
import java.util.Map;

/**
 * Execute class for shouldUpdate() function.
 */
public class IncrementalShouldUpdateFunctionExecutor
        extends FunctionExecutor<IncrementalShouldUpdateFunctionExecutor.FunctionState> {

    @Override
    protected StateFactory<FunctionState> init(ExpressionExecutor[] attributeExpressionExecutors,
                                               ConfigReader configReader, SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new SiddhiAppValidationException("shouldUpdate() function has to have exactly 1 parameter, " +
                    "currently " + attributeExpressionExecutors.length + " parameters provided");
        }
        if (attributeExpressionExecutors[0].getReturnType() != Attribute.Type.LONG) {
            throw new OperationNotSupportedException("Parameter given for shouldUpdate() function has to be of type " +
                    "long, but found: " + attributeExpressionExecutors[0].getReturnType());
        }
        return () -> new FunctionState();
    }

    @Override
    protected Object execute(Object[] data, FunctionState state) {
        //will not occur
        return null;
    }

    /**
     * return true/false based on timestamp values
     *
     * @param data  of Long type
     * @param state function state
     * @return true/false
     */
    @Override
    protected Object execute(Object data, FunctionState state) {
        long timestamp = (long) data;
        if (timestamp >= state.lastTimestamp) {
            state.lastTimestamp = timestamp;
            return true;
        }
        return false;
    }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.BOOL;
    }

    class FunctionState extends State {
        private long lastTimestamp = 0;

        @Override
        public boolean canDestroy() {
            return lastTimestamp == 0;
        }

        @Override
        public Map<String, Object> snapshot() {
            HashMap<String, Object> state = new HashMap<>();
            state.put("lastTimestamp", this.lastTimestamp);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            this.lastTimestamp = ((long) state.get("lastTimestamp"));
        }
    }
}
