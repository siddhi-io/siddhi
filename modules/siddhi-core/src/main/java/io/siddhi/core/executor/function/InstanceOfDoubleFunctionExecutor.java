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
 * Executor class for instanceOf Double function. Function execution logic is implemented in execute here.
 */
@Extension(
        name = "instanceOfDouble",
        namespace = "",
        description = "Checks whether the parameter is an instance of Double or not.",
        parameters = {
                @Parameter(name = "arg",
                        description = "The parameter to be checked.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT,
                                DataType.STRING, DataType.BOOL, DataType.OBJECT},
                        dynamic = true)
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"arg"})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returned type will be boolean and true if and only if the input " +
                        "is a instance of Double.",
                type = {DataType.BOOL}),
        examples = {
                @Example(
                        syntax = "from fooStream\n" +
                                "select instanceOfDouble(value) as state\n" +
                                "insert into barStream;",
                        description = "This will return true if the value field format is double ex : 56.45."
                ),
                @Example(
                        syntax = "from fooStream\n" +
                                "select instanceOfDouble(switchState) as state\n" +
                                "insert into barStream;",
                        description = "if the switchState = true then this will returns false as the value is not an" +
                                " instance of the double."
                )
        }
)
public class InstanceOfDoubleFunctionExecutor extends FunctionExecutor {

    Attribute.Type returnType = Attribute.Type.BOOL;

    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to instanceOfDouble() " +
                    "function, required only 1, but found " + attributeExpressionExecutors.length);
        }
        return null;
    }

    @Override
    protected Object execute(Object[] data, State state) {
        return null; //Since the instanceOfDouble function takes in 1 parameter, this method does not get called.
        // Hence, not implemented.
    }

    @Override
    protected Object execute(Object data, State state) {
        return data instanceof Double;
    }

    @Override
    public Attribute.Type getReturnType() {
        return returnType;
    }

}
