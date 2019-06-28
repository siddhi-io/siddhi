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
 * Executor class for instanceOf Long function. Function execution logic is implemented in execute here.
 */
@Extension(
        name = "instanceOfLong",
        namespace = "",
        description = "Checks whether the parameter is an instance of Long or not.",
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
                description = "Returned type will be boolean and true if and only if the input is a instance of Long.",
                type = {DataType.BOOL}),
        examples = {
                @Example(
                        syntax = "from fooStream\n" +
                                "select instanceOfLong(value) as state\n" +
                                "insert into barStream;",
                        description = "This will return true if the value field format is long ex : 56456l."
                ),
                @Example(
                        syntax = "from fooStream\n" +
                                "select instanceOfLong(switchState) as state\n" +
                                "insert into barStream;",
                        description = "if the switchState = true then this will returns false as the value is an" +
                                " instance of the boolean not a long."
                )
        }
)
public class InstanceOfLongFunctionExecutor extends FunctionExecutor {

    Attribute.Type returnType = Attribute.Type.BOOL;

    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to instanceOfLong() function, " +
                    "required only 1, but found " + attributeExpressionExecutors.length);
        }
        return null;
    }

    @Override
    protected Object execute(Object[] data, State state) {
        return null; //Since the instanceOfLong function takes in 1 parameter, this method does not get called. Hence,
        // not implemented.
    }

    @Override
    protected Object execute(Object data, State state) {
        return data instanceof Long;
    }

    @Override
    public Attribute.Type getReturnType() {
        return returnType;
    }

}
