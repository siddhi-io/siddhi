/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.core.executor.function;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ParameterOverload;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;

/**
 * Executor class for Maximum function. Function execution logic is implemented in execute here.
 */
@Extension(
        name = "maximum",
        namespace = "",
        description = "Returns the maximum value of the input parameters.",
        parameters = {
                @Parameter(name = "arg",
                        description = "This function accepts one or more parameters. " +
                                "They can belong to any one of the available types. " +
                                "All the specified parameters should be of the same type.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT},
                        dynamic = true)
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"arg", "..."})
        },
        returnAttributes = @ReturnAttribute(
                description = "This will be the same as the type of the first input parameter.",
                type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT}),
        examples = @Example(
                syntax = "@info(name = 'query1') from inputStream\n" +
                        "select maximum(price1, price2, price3) as max\n" +
                        "insert into outputStream;",
                description = "This will returns the maximum value of the input parameters price1, price2, price3."
        )
)
public class MaximumFunctionExecutor extends FunctionExecutor {

    private Attribute.Type returnType;

    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
        Attribute.Type attributeTypeOne = attributeExpressionExecutors[0].getReturnType();
        if (!((attributeTypeOne == Attribute.Type.DOUBLE) || (attributeTypeOne == Attribute.Type.INT) ||
                (attributeTypeOne == Attribute.Type.FLOAT) || (attributeTypeOne == Attribute.Type.LONG))) {
            throw new SiddhiAppValidationException("Invalid parameter type found for the argument" + 1 +
                    " of maximum() function, " +
                    "required " + Attribute.Type.INT + " or " + Attribute.Type.LONG +
                    " or " + Attribute.Type.FLOAT + " or " + Attribute.Type.DOUBLE +
                    ", but found " + attributeTypeOne.toString());
        }
        for (int i = 1; i < attributeExpressionExecutors.length; i++) {
            Attribute.Type attributeType = attributeExpressionExecutors[i].getReturnType();
            if (!((attributeType == Attribute.Type.DOUBLE) || (attributeType == Attribute.Type.INT) ||
                    (attributeType == Attribute.Type.FLOAT) || (attributeType == Attribute.Type.LONG))) {
                throw new SiddhiAppValidationException("Invalid parameter type found for the argument" + i +
                        " of maximum() function, " +
                        "required " + Attribute.Type.INT + " or " + Attribute.Type.LONG +
                        " or " + Attribute.Type.FLOAT + " or " + Attribute.Type.DOUBLE +
                        ", but found " + attributeType.toString());
            }
            if (attributeTypeOne != attributeType) {
                throw new SiddhiAppValidationException("Invalid parameter type found for arguments  " +
                        "of maximum() function, all parameters should be of same type, but found " +
                        attributeTypeOne + " and " + attributeExpressionExecutors[i].getReturnType());
            }

        }
        returnType = attributeTypeOne;
        return null;
    }

    /**
     * return maximum of arbitrary long set of Double values
     *
     * @param data  array of Double values
     * @param state
     * @return max
     */
    @Override
    protected Object execute(Object[] data, State state) {

        double max = Double.MIN_VALUE;
        for (Object aObj : data) {
            Double value = Double.MIN_VALUE;
            if (aObj instanceof Integer) {
                int inputInt = (Integer) aObj;
                value = (double) inputInt;
            } else if (aObj instanceof Long) {
                long inputLong = (Long) aObj;
                value = (double) inputLong;
            } else if (aObj instanceof Float) {
                float inputLong = (Float) aObj;
                value = (double) inputLong;
            } else if (aObj instanceof Double) {
                value = (Double) aObj;
            }
            if (value > max) {
                max = value;
            }
        }
        switch (returnType) {
            case INT:
                return (int) max;
            case LONG:
                return (long) max;
            case FLOAT:
                return (float) max;
            case DOUBLE:
                return max;
        }
        return max;
    }

    @Override
    protected Object execute(Object data, State state) {
        return data;
    }

    @Override
    public Attribute.Type getReturnType() {
        return returnType;
    }

}
