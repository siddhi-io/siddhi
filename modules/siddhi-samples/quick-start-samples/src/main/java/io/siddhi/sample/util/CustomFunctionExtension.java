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

package io.siddhi.sample.util;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.function.FunctionExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;

public class CustomFunctionExtension extends FunctionExecutor {

    private Attribute.Type returnType;

    /**
     * The initialization method for FunctionExecutor, this method will be called before the other methods
     *
     * @param attributeExpressionExecutors are the executors of each function parameters
     * @param configReader
     * @param siddhiQueryContext           the context of the siddhi query
     */
    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
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
     * The main execution method which will be called upon event arrival
     * when there are more then one function parameter
     *
     * @param data  the runtime values of function parameters
     * @param state
     * @return the function result
     */
    @Override
    protected Object execute(Object[] data, State state) {
        if (returnType == Attribute.Type.DOUBLE) {
            double total = 0;
            for (Object aObj : data) {
                total += Double.parseDouble(String.valueOf(aObj));
            }

            return total;
        } else {
            long total = 0;
            for (Object aObj : data) {
                total += Long.parseLong(String.valueOf(aObj));
            }
            return total;
        }
    }

    /**
     * The main execution method which will be called upon event arrival
     * when there are zero or one function parameter
     *
     * @param data  null if the function parameter count is zero or
     *              runtime data value of the function parameter
     * @param state Function state
     * @return the function result
     */
    @Override
    protected Object execute(Object data, State state) {
        if (returnType == Attribute.Type.DOUBLE) {
            return Double.parseDouble(String.valueOf(data));
        } else {
            return Long.parseLong(String.valueOf(data));
        }
    }

    @Override
    public Attribute.Type getReturnType() {
        return returnType;
    }

}
