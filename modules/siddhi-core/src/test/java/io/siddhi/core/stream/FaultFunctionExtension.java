/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.core.stream;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.function.FunctionExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;

@Extension(
        name = "fault",
        namespace = "custom",
        description = "Return the sum of the given input values.",
        parameters = {
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns a double.",
                type = {DataType.DOUBLE}),
        examples = @Example(
                syntax = "from fooStream\n" +
                        "select custom:fault() as total\n" +
                        "insert into barStream",
                description = "This throws an Runtime exception."
        )
)
public class FaultFunctionExtension extends FunctionExecutor {

    private Attribute.Type returnType;

    @Override
    public StateFactory init(ExpressionExecutor[] attributeExpressionExecutors,
                             ConfigReader configReader,
                             SiddhiQueryContext siddhiQueryContext) {
        returnType = Attribute.Type.DOUBLE;
        return null;
    }

    /**
     * Return type of the custom function mentioned
     *
     * @return return type
     */
    @Override
    public Attribute.Type getReturnType() {
        return returnType;
    }


    @Override
    protected Object execute(Object[] obj, State state) {
        throw new RuntimeException("Error when running faultAdd()");

    }

    @Override
    protected Object execute(Object obj, State state) {
        throw new RuntimeException("Error when running faultAdd()");
    }

}
