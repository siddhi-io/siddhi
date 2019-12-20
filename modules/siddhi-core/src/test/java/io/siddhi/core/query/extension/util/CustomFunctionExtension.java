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
package io.siddhi.core.query.extension.util;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.function.FunctionExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;

@Extension(
        name = "plus",
        namespace = "custom",
        description = "Return the sum of the given input values.",
        parameters = {
                @Parameter(name = "args",
                        description = "The values that need to be sum.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returns the calculated sum value as a double or float.",
                type = {DataType.DOUBLE, DataType.FLOAT}),
        examples = @Example(
                syntax = "from fooStream\n" +
                        "select custom:plus(4, 6, 10) as total\n" +
                        "insert into barStream",
                description = "This will return value 20 as total."
        )
)
public class CustomFunctionExtension extends FunctionExecutor {

    private Attribute.Type returnType;

    @Override
    public StateFactory init(ExpressionExecutor[] attributeExpressionExecutors,
                             ConfigReader configReader,
                             SiddhiQueryContext siddhiQueryContext) {
        for (ExpressionExecutor expressionExecutor : attributeExpressionExecutors) {
            Attribute.Type attributeType = expressionExecutor.getReturnType();
            if (attributeType == Attribute.Type.DOUBLE) {
                returnType = attributeType;

            } else if ((attributeType == Attribute.Type.STRING) || (attributeType == Attribute.Type.BOOL)) {
                throw new SiddhiAppCreationException("Plus cannot have parameters with types String or Bool");
            } else {
                returnType = Attribute.Type.LONG;
            }
        }
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
        if (returnType == Attribute.Type.DOUBLE) {
            double total = 0;
            for (Object aObj : obj) {
                total += Double.parseDouble(String.valueOf(aObj));
            }

            return total;
        } else {
            long total = 0;
            for (Object aObj : obj) {
                total += Long.parseLong(String.valueOf(aObj));
            }
            return total;
        }

    }

    @Override
    protected Object execute(Object obj, State state) {
        if (returnType == Attribute.Type.DOUBLE) {
            double total = 0;
            if (obj instanceof Object[]) {
                for (Object aObj : (Object[]) obj) {
                    total += Double.parseDouble(String.valueOf(aObj));
                }
            }
            return total;
        } else {
            long total = 0;
            if (obj instanceof Object[]) {
                for (Object aObj : (Object[]) obj) {
                    total += Long.parseLong(String.valueOf(aObj));
                }
            }
            return total;
        }
    }

}
