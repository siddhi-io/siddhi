/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import io.siddhi.core.exception.OperationNotSupportedException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.HashSet;
import java.util.Set;

/**
 * Executor class for createSet function. Function execution logic is implemented in execute here.
 */
@Extension(
        name = "createSet",
        namespace = "",
        description = "Includes the given input parameter in a java.util.HashSet and returns the set. ",
        parameters = {
                @Parameter(name = "input",
                        description = "The input that needs to be added into the set.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE,
                                DataType.FLOAT, DataType.STRING, DataType.BOOL},
                        dynamic = true)
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"input"})
        },
        returnAttributes = @ReturnAttribute(
                description = "The set that includes the input element.",
                type = {DataType.OBJECT}),
        examples = @Example(
                syntax = "from stockStream \n" +
                        "select createSet(symbol) as initialSet \n" +
                        "insert into initStream;",
                description = "For every incoming stockStream event, the initStream stream will produce a set object " +
                        "having only one element: the symbol in the incoming stockStream."
        )
)
public class CreateSetFunctionExecutor extends FunctionExecutor {

    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new SiddhiAppValidationException("createSet() function has to have exactly 1 parameter, currently " +
                    attributeExpressionExecutors.length + " parameters provided");
        }
        if (!isAttributeTypeSupported(attributeExpressionExecutors[0].getReturnType())) {
            throw new OperationNotSupportedException("createSet() function not supported for type: " +
                    attributeExpressionExecutors[0].getReturnType());
        }
        return null;
    }

    @Override
    protected Object execute(Object[] data, State state) {
        return null; //Since the createSet function takes in only 1 parameter, this method does not get called.
        // Hence, not implemented.
    }

    /**
     * return set object, containing only one element: data.
     *
     * @param data  array of Double values
     * @param state
     * @return the set object
     */
    @Override
    protected Object execute(Object data, State state) {
        Set set = new HashSet();
        set.add(data);
        return set;
    }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.OBJECT;
    }

    private boolean isAttributeTypeSupported(Attribute.Type type) {
        switch (type) {
            case FLOAT:
                return true;
            case INT:
                return true;
            case LONG:
                return true;
            case DOUBLE:
                return true;
            case STRING:
                return true;
            case BOOL:
                return true;
            default:
                return false;
        }
    }
}
