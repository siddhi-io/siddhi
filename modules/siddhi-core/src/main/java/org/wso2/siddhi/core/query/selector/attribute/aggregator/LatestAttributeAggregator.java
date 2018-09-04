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
 * {@link AttributeAggregator} to calculate latest value based on timestamp event attribute.
 */
@Extension(
        namespace = "",
        name = "latest",
        description = "Returns the latest value of the attribute based on the time stamp attribute",
        parameters = {
                @Parameter(
                        name = "arg",
                        description = "The value that needs to be compared to find the latest value.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT}
                ),
                @Parameter(
                        name = "timestamp",
                        description = "The timestamp value used to calculate latest.",
                        type = {DataType.LONG}
                )
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns the maximum value in the same data type as the input.",
                type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT}),
        examples = @Example(
                syntax = "from fooStream#window.timeBatch(1 min)\n" +
                        "select max(temp, timestamp) as latestTemp\n" +
                        "insert into barStream;",
                description = "latest(temp, timestamp) returns the latest temp value " +
                        "recorded for all the events based on the timestamp value."
        )
)
public class LatestAttributeAggregator extends AttributeAggregator {

    private Attribute.Type returnType;
    private long latestTimestamp;
    private Object latestValue;

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                        SiddhiAppContext siddhiAppContext) {
        if (attributeExpressionExecutors.length != 2) {
            throw new OperationNotSupportedException("Latest aggregator has to have exactly 2 parameter, currently '" +
                    attributeExpressionExecutors.length + "' parameters provided");
        }

        if (attributeExpressionExecutors[1].getReturnType() != Attribute.Type.LONG) {
            throw new OperationNotSupportedException("The timestamp parameter of the latest aggregator " +
                    "has to be of type `LONG`, currently '" + attributeExpressionExecutors.length + "' type provided");
        }
        this.returnType = attributeExpressionExecutors[0].getReturnType();
        this.latestTimestamp = 0;
    }

    @Override
    public Attribute.Type getReturnType() {
        return this.returnType;
    }

    @Override
    public Object processAdd(Object data) {
        return new IllegalStateException("Latest can ONLY process data array, but found " + data.toString());
    }

    @Override
    public Object processAdd(Object[] data) {
        long timestamp = (long) data[1];
        if (this.latestTimestamp < timestamp) {
            this.latestTimestamp = timestamp;
            this.latestValue = data[0];
        }
        return this.latestValue;
    }

    @Override
    public Object processRemove(Object data) {
        return new IllegalStateException("Latest can ONLY process data array, but found " + data.toString());
    }

    @Override
    public Object processRemove(Object[] data) {
        long timestamp = (long) data[1];
        if (this.latestTimestamp > timestamp) {
            this.latestTimestamp = timestamp;
            this.latestValue = data[0];
        }
        return this.latestValue;
    }

    @Override
    public boolean canDestroy() {
        return true;
    }

    @Override
    public Object reset() {
        this.latestTimestamp = 0;
        this.latestValue = null;
        return null;
    }

    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> state = new HashMap<>();
        state.put("latestTimestamp", latestTimestamp);
        state.put("latestValue", latestValue);
        return state;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        this.latestTimestamp = ((long) state.get("latestTimestamp"));
        this.latestValue = state.get("latestValue");
    }
}
