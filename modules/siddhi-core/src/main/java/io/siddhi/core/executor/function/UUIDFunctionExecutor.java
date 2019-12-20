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
import io.siddhi.annotation.ParameterOverload;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;

import java.util.UUID;

/**
 * Executor class for UUID function. Function execution logic is implemented in execute here.
 */
@Extension(
        name = "UUID",
        namespace = "",
        description = "Generates a UUID (Universally Unique Identifier).",
        parameters = {},
        parameterOverloads = {
                @ParameterOverload()
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns a UUID string.",
                type = {DataType.STRING}),
        examples = @Example(
                syntax = "from TempStream\n" +
                        "select convert(roomNo, 'string') as roomNo, temp, UUID() as messageID\n" +
                        "insert into RoomTempStream;",
                description = "This will converts a room number to string, introducing a message ID to each event as" +
                        "UUID() returns a34eec40-32c2-44fe-8075-7f4fde2e2dd8\n" +
                        "\n" +
                        "from TempStream\n" +
                        "select convert(roomNo, 'string') as roomNo, temp, UUID() as messageID\n" +
                        "insert into RoomTempStream;"
        )
)
public class UUIDFunctionExecutor extends FunctionExecutor {

    Attribute.Type returnType = Attribute.Type.STRING;

    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors,
                                ConfigReader configReader, SiddhiQueryContext siddhiQueryContext) {
        //Nothing to be done.
        return null;
    }

    @Override
    protected Object execute(Object[] data, State state) {
        return null; //Since the e function takes in no parameters, this method does not get called. Hence, not
        // implemented.
    }

    @Override
    protected Object execute(Object data, State state) {
        return UUID.randomUUID().toString();
    }

    @Override
    public Attribute.Type getReturnType() {
        return returnType;
    }

}
