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
package org.wso2.siddhi.core.query.selector.attribute.aggregator;

import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.HashMap;
import java.util.Map;

/**
 * {@link AttributeAggregator} to calculate sum based on an event attribute.
 */
@Extension(
        name = "or",
        namespace = "",
        description = "Returns the results of OR operation for all the events.",
        parameters = {
                @Parameter(name = "arg",
                        description = "The value that needs to be OR operation.",
                        type = {DataType.BOOL})
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
public class OrAttributeAggregator extends AttributeAggregator {

    private static Attribute.Type type = Attribute.Type.BOOL;
    private int trueEventsCount = 0;

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param configReader                 this hold the {@link OrAttributeAggregator} configuration reader.
     * @param siddhiAppContext             Siddhi app runtime context
     */
    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                        SiddhiAppContext siddhiAppContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new OperationNotSupportedException("And aggregator has to have exactly 1 parameter, currently " +
                    attributeExpressionExecutors.length
                    + " parameters provided");
        }
    }

    public Attribute.Type getReturnType() {
        return type;
    }

    @Override
    public Object processAdd(Object data) {
        if ((boolean) data) {
            trueEventsCount++;
        }
        return computeLogicalOperation();
    }

    @Override
    public Object processAdd(Object[] data) {
        for (Object object : data) {
            if ((boolean) object) {
                trueEventsCount++;
            }
        }
        return computeLogicalOperation();
    }

    @Override
    public Object processRemove(Object data) {
        if ((boolean) data) {
            trueEventsCount--;
        }
        return computeLogicalOperation();
    }

    @Override
    public Object processRemove(Object[] data) {
        for (Object object : data) {
            if ((boolean) object) {
                trueEventsCount--;
            }
        }
        return computeLogicalOperation();
    }

    private boolean computeLogicalOperation() {
        return trueEventsCount > 0;
    }

    @Override
    public Object reset() {
        trueEventsCount = 0;
        return false;
    }

    @Override
    public boolean canDestroy() {
        return trueEventsCount == 0;
    }

    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> state = new HashMap<>();
        state.put("trueEventsCount", trueEventsCount);
        return state;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        trueEventsCount = (int) state.get("trueEventsCount");
    }
}
