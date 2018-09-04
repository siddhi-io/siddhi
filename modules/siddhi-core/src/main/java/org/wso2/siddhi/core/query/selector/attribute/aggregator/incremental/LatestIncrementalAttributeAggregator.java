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
package org.wso2.siddhi.core.query.selector.attribute.aggregator.incremental;


import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.List;

/**
 * {@link IncrementalAttributeAggregator} to calculate value in last arrived event in the aggregation granularity.
 */
@Extension(
        namespace = "incrementalAggregator",
        name = "latest",
        description = "Returns the the latest value based on timestamp attribute",
        parameters = {
                @Parameter(name = "arg",
                        description = "The attribute for which latest will be calculated.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE,
                                DataType.FLOAT, DataType.STRING, DataType.BOOL}),
                @Parameter(name = "timestamp",
                        description = "Timestamp attribute through which latest is determined.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE,
                                DataType.FLOAT, DataType.STRING, DataType.BOOL})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns the latest value of the attribute.",
                type = {DataType.INT, DataType.LONG, DataType.DOUBLE,
                        DataType.FLOAT, DataType.STRING, DataType.BOOL}),
        examples = @Example(
                syntax = " define aggregation cseEventAggregation\n from cseEventStream\n" +
                        " select latest(price, timestamp) as countEvents,\n " +
                        "aggregate by timeStamp every sec ... hour;",
                description = "latest(symbol) returns the latest value of price based on their " +
                        "event timestamp. The latest(symbol) is calculated for sec, min and hour durations."
        )
)
public class LatestIncrementalAttributeAggregator extends IncrementalAttributeAggregator {

    private Attribute[] baseAttributes;
    private Expression[] baseAttributesInitialValues;
    private Attribute.Type returnType;

    @Override
    public void init(List<Attribute> attributeList) {

        if (attributeList == null) {
            throw new SiddhiAppCreationException("Latest Incremental attribute aggregation cannot be executed when " +
                    "no parameters are given.");
        }
        if (attributeList.size() != 2) {
            throw new SiddhiAppCreationException("Latest Incremental attribute aggregations can be executed ONLY " +
                    "when 2 attributes are given.");
        }

        Attribute variableAttribute = attributeList.get(0);
        MaxIncrementalAttributeAggregator maxAttributeAggregator = new MaxIncrementalAttributeAggregator();
        maxAttributeAggregator.init(attributeList.subList(1, 2));

        this.baseAttributes = new Attribute[]{maxAttributeAggregator.getBaseAttributes()[0], variableAttribute};
        this.baseAttributesInitialValues = new Expression[]
                {
                        maxAttributeAggregator.getBaseAttributeInitialValues()[0],
                        Expression.variable(variableAttribute.getName())
                };
        this.returnType = variableAttribute.getType();
    }

    @Override
    public Expression aggregate() {
        return Expression.variable(getBaseAttributes()[1].getName());
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
        Expression maxTimestamp = Expression.function("max",
                Expression.variable(getBaseAttributes()[0].getName()));
        Expression conditionalVariable = Expression.function("latest",
                Expression.variable(getBaseAttributes()[1].getName()),
                Expression.variable(getBaseAttributes()[0].getName())
                );
        return new Expression[]{maxTimestamp, conditionalVariable};
    }

    @Override
    public Attribute.Type getReturnType() {
        return this.returnType;
    }
}
