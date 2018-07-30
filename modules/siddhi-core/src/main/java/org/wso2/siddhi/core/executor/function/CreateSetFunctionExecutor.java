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

package org.wso2.siddhi.core.executor.function;

import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.HashSet;
import java.util.Map;
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
                                DataType.FLOAT, DataType.STRING, DataType.BOOL})
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
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                        SiddhiAppContext siddhiAppContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new SiddhiAppValidationException("createSet() function has to have exactly 1 parameter, currently " +
                    attributeExpressionExecutors.length + " parameters provided");
        }
        if (!isAttributeTypeSupported(attributeExpressionExecutors[0].getReturnType())) {
            throw new OperationNotSupportedException("createSet() function not supported for type: " +
                    attributeExpressionExecutors[0].getReturnType());
        }
    }

    @Override
    protected Object execute(Object[] data) {
        return null; //Since the createSet function takes in only 1 parameter, this method does not get called.
        // Hence, not implemented.
    }

    /**
     * return set object, containing only one element: data.
     *
     * @param data array of Double values
     * @return the set object
     */
    @Override
    protected Object execute(Object data) {
        Set set = new HashSet();
        set.add(data);
        return set;
    }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.OBJECT;
    }

    @Override
    public Map<String, Object> currentState() {
        return null;    //no state is maintained.
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        //Does nothing as no state is maintained.
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
