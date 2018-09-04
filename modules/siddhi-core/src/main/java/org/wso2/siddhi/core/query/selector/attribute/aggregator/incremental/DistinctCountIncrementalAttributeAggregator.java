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

import java.util.List;

/**
 * {@link IncrementalAttributeAggregator} to calculate count based on an event attribute.
 */
@Extension(
        name = "distinctCount",
        namespace = "incrementalAggregator",
        description = "Returns the count of all events, in incremental event processing",
        parameters = {
                @Parameter(name = "arg",
                        description = "The attribute that needs to be counted.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE,
                                DataType.FLOAT, DataType.STRING, DataType.BOOL})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns the distinct event count as a long.",
                type = {DataType.LONG}),
        examples = @Example(
                syntax = " define aggregation cseEventAggregation\n from cseEventStream\n" +
                        " select distinctCount(symbol) as countEvents,\n aggregate by timeStamp every sec ... hour;",
                description = "distinctCount(symbol) returns the distinct count of all the symbols based on their " +
                        "arrival and expiry. The count is calculated for sec, min and hour durations."
        )
)
public class DistinctCountIncrementalAttributeAggregator extends IncrementalAttributeAggregator {

    private Attribute[] baseAttributes;
    private Expression[] baseAttributesInitialValues;

    @Override
    public void init(List<Attribute> attributeList) {
        Attribute set;
        Expression setInitialValue;

        if (attributeList.get(0) == null) {
            throw new SiddhiAppCreationException("Distinct incremental attribute aggregation cannot be executed " +
                    "when no parameters are given");
        }

        String attributeName = attributeList.get(0).getName();
        Attribute.Type attributeType = attributeList.get(0).getType();
        // distinct-count is not supported for object types.
        if (attributeType.equals(Attribute.Type.FLOAT) || attributeType.equals(Attribute.Type.DOUBLE)
                || attributeType.equals(Attribute.Type.INT) || attributeType.equals(Attribute.Type.LONG)
                || attributeType.equals(Attribute.Type.STRING) || attributeType.equals(Attribute.Type.BOOL)) {
            set = new Attribute("AGG_SET_".concat(attributeName), Attribute.Type.OBJECT);
            setInitialValue = Expression.function("createSet", Expression.variable(attributeName));
        } else {
            throw new SiddhiAppRuntimeException(
                    "Distinct count aggregation cannot be executed on attribute type " + attributeType.toString());
        }

        this.baseAttributes = new Attribute[]{set};
        this.baseAttributesInitialValues = new Expression[]{setInitialValue};
    }

    @Override
    public Expression aggregate() {
        return Expression.function("sizeOfSet", Expression.variable(baseAttributes[0].getName()));
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
        Expression setAggregator = Expression.function("unionSet",
                Expression.variable(getBaseAttributes()[0].getName()));
        return new Expression[]{setAggregator};
    }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.LONG;
    }
}

