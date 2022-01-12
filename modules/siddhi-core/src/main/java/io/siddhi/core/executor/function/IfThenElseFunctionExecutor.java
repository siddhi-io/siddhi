/*
 *  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Executor class for ifThenElse function. Function execution logic is implemented in execute here.
 */
@Extension(
        name = "ifThenElse",
        namespace = "",
        description = "Evaluates the 'condition' parameter and returns value of the 'if.expression' parameter " +
                "if the condition is true, or returns value of the 'else.expression' parameter if the condition is "
                + "false. Here both 'if.expression' and 'else.expression' should be of the same type.",
        parameters = {
                @Parameter(name = "condition",
                        description = "This specifies the if then else condition value.",
                        type = {DataType.BOOL},
                        dynamic = true),
                @Parameter(name = "if.expression",
                        description = "This specifies the value to be returned if " +
                                "the value of the condition parameter is true.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT,
                                DataType.STRING, DataType.BOOL, DataType.OBJECT},
                        dynamic = true),
                @Parameter(name = "else.expression",
                        description = "This specifies the value to be returned if " +
                                "the value of the condition parameter is false.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT,
                                DataType.STRING, DataType.BOOL, DataType.OBJECT},
                        dynamic = true)
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"condition", "if.expression", "else.expression"})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returned type will be same as the 'if.expression' and 'else.expression' type.",
                type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT,
                        DataType.STRING, DataType.BOOL, DataType.OBJECT}),
        examples = {
                @Example(
                        syntax = "@info(name = 'query1')\n" +
                                "from sensorEventStream\n" +
                                "select sensorValue, ifThenElse(sensorValue>35,'High','Low') as status\n" +
                                "insert into outputStream;",
                        description = "This will returns High if sensorValue = 50."
                ),
                @Example(
                        syntax = "@info(name = 'query1')\n" +
                                "from sensorEventStream\n" +
                                "select sensorValue, ifThenElse(voltage < 5, 0, 1) as status\n" +
                                "insert into outputStream;",
                        description = "This will returns 1 if voltage= 12."
                ),
                @Example(
                        syntax = "@info(name = 'query1')\n" +
                                "from userEventStream\n" +
                                "select userName, ifThenElse(password == 'admin', true, false) as passwordState\n" +
                                "insert into outputStream;",
                        description = "This will returns  passwordState as true if password = admin."
                )
        }
)
public class IfThenElseFunctionExecutor extends FunctionExecutor {
    private static final Logger log = LogManager.getLogger(IfThenElseFunctionExecutor.class);
    Attribute.Type returnType;

    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors,
                                ConfigReader configReader, SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length != 3) {
            // check whether all the arguments passed
            throw new SiddhiAppValidationException("Invalid no of arguments passed to ifThenElse() function, " +
                    "required only 3, but found " + attributeExpressionExecutors.length);
        } else if (!attributeExpressionExecutors[0].getReturnType().equals(Attribute.Type.BOOL)) {
            // check whether first argument Boolean or not
            throw new SiddhiAppValidationException("Input type of if in ifThenElse function should be of " +
                    "type BOOL, but found " + attributeExpressionExecutors[0].getReturnType());
        } else if (!attributeExpressionExecutors[1].getReturnType().equals(
                attributeExpressionExecutors[2].getReturnType())) {
            // check whether second and thirds argument's return type are equivalent.
            throw new SiddhiAppValidationException("Input type of then in ifThenElse function and else in " +
                    "ifThenElse function should be of equivalent type. but found then type: " +
                    attributeExpressionExecutors[1].getReturnType() + " and else type: " +
                    attributeExpressionExecutors[2].getReturnType());
        } else {
            returnType = attributeExpressionExecutors[1].getReturnType();
        }
        return null;
    }

    @Override
    protected Object execute(Object[] data, State state) {
        // check whether first argument true or null
        if (Boolean.TRUE.equals(data[0])) {
            return data[1];
        } else {
            return data[2];
        }
    }

    @Override
    protected Object execute(Object data, State state) {
        // Since the e function takes in multiple parameters, this method does not get called. Hence, not implemented.
        return null;
    }

    @Override
    public Attribute.Type getReturnType() {
        return returnType;
    }

    @Override
    public Object execute(ComplexEvent event) {
        try {
            Boolean condition = Boolean.TRUE.equals(attributeExpressionExecutors[0].execute(event));
            return execute(
                    new Object[]{
                            condition,
                            (condition) ? attributeExpressionExecutors[1].execute(event) : null,
                            (!condition) ? attributeExpressionExecutors[2].execute(event) : null
                    });
        } catch (Exception e) {
            throw new SiddhiAppRuntimeException(e.getMessage() + ". Exception on class '" + this.getClass().getName()
                    + "'", e);
        }
    }
}
