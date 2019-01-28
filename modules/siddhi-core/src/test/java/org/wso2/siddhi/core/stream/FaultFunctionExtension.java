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
package org.wso2.siddhi.core.stream;

import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.Map;

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
                description = "This will return value 20.0 as total."
        )
)
public class FaultFunctionExtension extends FunctionExecutor {

    private Attribute.Type returnType;

    @Override
    public void init(ExpressionExecutor[] attributeExpressionExecutors,
                     ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {
        returnType = Attribute.Type.DOUBLE;
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
    protected Object execute(Object[] obj) {
        throw new RuntimeException("Error when running faultAdd()");

    }

    @Override
    protected Object execute(Object obj) {
        throw new RuntimeException("Error when running faultAdd()");
    }

    @Override
    public Map<String, Object> currentState() {
        //No state
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        //Nothing to be done
    }
}
