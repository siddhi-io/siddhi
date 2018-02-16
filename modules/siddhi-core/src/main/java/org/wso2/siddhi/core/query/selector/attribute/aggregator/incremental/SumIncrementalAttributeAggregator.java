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
 * {@link IncrementalAttributeAggregator} to calculate sum based on an event attribute.
 */
@Extension(
        name = "sum",
        namespace = "incrementalAggregator",
        description = "Returns the sum for all the events, in incremental event processing",
        parameters = {
                @Parameter(name = "arg",
                        description = "The value that needs to be summed.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns long if the input parameter type is int or long, and returns double if the " +
                        "input parameter type is float or double.",
                type = {DataType.LONG, DataType.DOUBLE}),
        examples = @Example(
                syntax = " define aggregation cseEventAggregation\n from cseEventStream\n" +
                        " select sum(price) as totalPrice,\n aggregate by timeStamp every sec ... hour;",
                description = "sum(price) returns the total price value for all the events based on their " +
                        "arrival and expiry. The total is calculated for sec, min and hour durations."
        )
)
public class SumIncrementalAttributeAggregator extends IncrementalAttributeAggregator {

    private Attribute[] baseAttributes;
    private Expression[] baseAttributesInitialValues;
    private Attribute.Type returnType;

    @Override
    public void init(String attributeName, Attribute.Type attributeType) {
        Attribute sum;
        Expression sumInitialValue;

        if (attributeName == null) {
            throw new SiddhiAppCreationException("Sum incremental attribute aggregation cannot be executed " +
                    "when no parameters are given");
        }

        if (attributeType.equals(Attribute.Type.FLOAT) || attributeType.equals(Attribute.Type.DOUBLE)) {
            sum = new Attribute("AGG_SUM_".concat(attributeName), Attribute.Type.DOUBLE);
            sumInitialValue = Expression.function("convert", Expression.variable(attributeName),
                    Expression.value("double"));
            returnType = Attribute.Type.DOUBLE;
        } else if (attributeType.equals(Attribute.Type.INT) || attributeType.equals(Attribute.Type.LONG)) {
            sum = new Attribute("AGG_SUM_".concat(attributeName), Attribute.Type.LONG);
            sumInitialValue = Expression.function("convert", Expression.variable(attributeName),
                    Expression.value("long"));
            returnType = Attribute.Type.LONG;
        } else {
            throw new SiddhiAppRuntimeException(
                    "Sum aggregation cannot be executed on attribute type " + attributeType.toString());
        }
        this.baseAttributes = new Attribute[]{sum};
        this.baseAttributesInitialValues = new Expression[]{sumInitialValue}; // Original attribute names
        // used for initial values, since those would be executed using original meta

        assert baseAttributes.length == baseAttributesInitialValues.length;
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
        Expression sumAggregator = Expression.function("sum",
                Expression.variable(getBaseAttributes()[0].getName()));
        return new Expression[]{sumAggregator};
    }

    @Override
    public Attribute.Type getReturnType() {
        return returnType;
    }
}
