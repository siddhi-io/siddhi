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
package org.wso2.siddhi.core.executor.incremental;

import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.HashMap;
import java.util.Map;

/**
 * Execute class for shouldUpdate() function.
 */
public class IncrementalShouldUpdateFunctionExecutor extends FunctionExecutor {
    private long lastTimestamp = 0;

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                        SiddhiAppContext siddhiAppContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new SiddhiAppValidationException("shouldUpdate() function has to have exactly 1 parameter, " +
                    "currently " + attributeExpressionExecutors.length + " parameters provided");
        }
        if (attributeExpressionExecutors[0].getReturnType() != Attribute.Type.LONG) {
            throw new OperationNotSupportedException("Parameter given for shouldUpdate() function has to be of type " +
                    "long, but found: " + attributeExpressionExecutors[0].getReturnType());
        }
    }

    @Override
    protected Object execute(Object[] data) {
        //will not occur
        return null;
    }

    /**
     * return true/false based on timestamp values
     *
     * @param data of Long type
     * @return true/false
     */
    @Override
    protected Object execute(Object data) {
        long timestamp = (long) data;
        if (timestamp >= this.lastTimestamp) {
            this.lastTimestamp = timestamp;
            return true;
        }
        return false;
    }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.BOOL;
    }

    @Override
    public Map<String, Object> currentState() {
        HashMap<String, Object> state = new HashMap<>();
        state.put("lastTimestamp", this.lastTimestamp);
        return state;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        this.lastTimestamp = ((long) state.get("lastTimestamp"));
    }
}
