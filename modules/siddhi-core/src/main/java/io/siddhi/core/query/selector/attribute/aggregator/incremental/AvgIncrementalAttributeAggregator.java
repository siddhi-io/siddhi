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

package io.siddhi.core.query.selector.attribute.aggregator.incremental;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ParameterOverload;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.expression.Expression;

/**
 * {@link IncrementalAttributeAggregator} to calculate average based on an event attribute.
 */
@Extension(
        name = "avg",
        namespace = "incrementalAggregator",
        description = "Defines the logic to calculate the average, in incremental event processing",
        parameters = {
                @Parameter(name = "arg",
                        description = "The value that needs to be averaged incrementally, for different durations.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT},
                        dynamic = true)
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"arg"})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns the calculated average value as a double.",
                type = {DataType.DOUBLE}),
        examples = @Example(
                syntax = " define aggregation cseEventAggregation\n from cseEventStream\n" +
                        " select avg(price) as avgPrice,\n aggregate by timeStamp every sec ... hour;",
                description = "avg(price) returns the average price value for all the events based on their " +
                        "arrival and expiry. The average is calculated for sec, min and hour durations."
        )
)
public class AvgIncrementalAttributeAggregator extends IncrementalAttributeAggregator {

    private Attribute[] baseAttributes;
    private Expression[] baseAttributesInitialValues;

    @Override
    public void init(String attributeName, Attribute.Type attributeType) {
        // Send the relevant attribute to this
        Attribute sum;
        Attribute count;
        Expression sumInitialValue;
        Expression countInitialValue;

        if (attributeName == null) {
            throw new SiddhiAppCreationException("Average incremental attribute aggregation cannot be executed " +
                    "when no parameters are given");
        }

        SumIncrementalAttributeAggregator sumIncrementalAttributeAggregator = new SumIncrementalAttributeAggregator();
        sumIncrementalAttributeAggregator.init(attributeName, attributeType);
        CountIncrementalAttributeAggregator countIncrementalAttributeAggregator =
                new CountIncrementalAttributeAggregator();
        countIncrementalAttributeAggregator.init(null, null);

        // Only one attribute exists for sum and count
        sum = sumIncrementalAttributeAggregator.getBaseAttributes()[0];
        count = countIncrementalAttributeAggregator.getBaseAttributes()[0];

        // Only one init value exists for sum and count
        sumInitialValue = sumIncrementalAttributeAggregator.getBaseAttributeInitialValues()[0];
        countInitialValue = countIncrementalAttributeAggregator.getBaseAttributeInitialValues()[0];

        this.baseAttributes = new Attribute[]{sum, count};
        this.baseAttributesInitialValues = new Expression[]{sumInitialValue, countInitialValue};
    }

    @Override
    public Expression aggregate() {
        return Expression.divide(Expression.variable(baseAttributes[0].getName()),
                Expression.variable(baseAttributes[1].getName()));
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
        Expression countAggregator = Expression.function("sum",
                Expression.variable(getBaseAttributes()[1].getName()));
        return new Expression[]{sumAggregator, countAggregator};
    }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.DOUBLE;
    }

}
