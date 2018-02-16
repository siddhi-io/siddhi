/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.core.query.selector.attribute.aggregator.incremental;

import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.expression.Expression;

/**
 * {@link IncrementalAttributeAggregator} to calculate minimum value based on an event attribute.
 */
@Extension(
        name = "min",
        namespace = "incrementalAggregator",
        description = "Returns the minimum value for all the events, in incremental event processing",
        parameters = {
                @Parameter(name = "arg",
                        description = "The value that needs to be compared to find the minimum value.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns the minimum value in the same data type as the input.",
                type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT}),
        examples = @Example(
                syntax = " define aggregation cseEventAggregation\n from cseEventStream\n" +
                        " select min(price) as minPrice,\n aggregate by timeStamp every sec ... hour;",
                description = "min(price) returns the minimum price value for all the events based on their " +
                        "arrival and expiry. The minimum value is calculated for sec, min and hour durations."
        )
)
public class MinIncrementalAttributeAggregator extends IncrementalAttributeAggregator {

    private Attribute[] baseAttributes;
    private Expression[] baseAttributesInitialValues;
    private Attribute.Type returnType;

    @Override
    public void init(String attributeName, Attribute.Type attributeType) {

        if (attributeName == null) {
            throw new SiddhiAppCreationException("Min incremental attribute aggregation cannot be executed " +
                    "when no parameters are given");
        }

        if (attributeType.equals(Attribute.Type.INT) || attributeType.equals(Attribute.Type.LONG) ||
                attributeType.equals(Attribute.Type.DOUBLE) || attributeType.equals(Attribute.Type.FLOAT)) {
            this.baseAttributes = new Attribute[]{new Attribute("AGG_MIN_".concat(attributeName), attributeType)};
            this.baseAttributesInitialValues = new Expression[]{Expression.variable(attributeName)};
            this.returnType = attributeType;

            assert baseAttributes.length == baseAttributesInitialValues.length;
        } else {
            throw new SiddhiAppRuntimeException(
                    "Min aggregation cannot be executed on attribute type " + attributeType.toString());
        }
    }

    @Override
    public Expression aggregate() {
        return Expression.variable(baseAttributes[0].getName());
    }

    @Override
    public Attribute[] getBaseAttributes() {
        return this.baseAttributes;
    }

    @Override
    public Expression[] getBaseAttributeInitialValues() {
        return this.baseAttributesInitialValues;
    }

    @Override
    public Expression[] getBaseAggregators() {
        Expression minAggregator = Expression.function("min",
                Expression.variable(getBaseAttributes()[0].getName()));
        return new Expression[]{minAggregator};
    }

    @Override
    public Attribute.Type getReturnType() {
        return this.returnType;
    }
}
