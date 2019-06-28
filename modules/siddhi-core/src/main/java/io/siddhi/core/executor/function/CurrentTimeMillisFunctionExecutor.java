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
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.timestamp.TimestampGenerator;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;

/**
 * Executor class for getting Siddhi application timestamp.
 */
@Extension(
        name = "currentTimeMillis",
        namespace = "",
        description = "Returns the current timestamp of siddhi application in milliseconds.",
        parameters = {},
        parameterOverloads = {
                @ParameterOverload()
        },
        returnAttributes = @ReturnAttribute(
                description = "siddhi application's current timestamp in milliseconds.",
                type = DataType.LONG),
        examples = {
                @Example(
                        syntax = "from fooStream\n" +
                                "select symbol as name, currentTimeMillis() as eventTimestamp \n" +
                                "insert into barStream;",
                        description = "This will extract current siddhi application timestamp.")
        }
)
public class CurrentTimeMillisFunctionExecutor extends FunctionExecutor {

    private TimestampGenerator timestampGenerator;

    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length != 0) {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to eventTimestamp() function, " +
                    "required 0 parameters, but found " +
                    attributeExpressionExecutors.length);
        }
        timestampGenerator = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator();
        return null;
    }

    @Override
    public Object execute(ComplexEvent event) {
        return timestampGenerator.currentTime();
    }

    @Override
    protected Object execute(Object[] data, State state) {
        //will not occur
        return null;
    }

    @Override
    protected Object execute(Object data, State state) {
        //will not occur
        return null;
    }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.LONG;
    }
}


