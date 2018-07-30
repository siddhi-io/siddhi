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
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.Map;
import java.util.Set;

/**
 * Executor class for sizeOfSet function. Function execution logic is implemented in execute here.
 */
@Extension(
        name = "sizeOfSet",
        namespace = "",
        description = "Returns the size of an object of type java.util.Set.",
        parameters = {
                @Parameter(name = "set",
                        description = "The set object. " +
                                "This parameter should be of type java.util.Set. " +
                                "A set object may be created by the 'set' attribute aggregator in Siddhi. ",
                        type = {DataType.OBJECT})
        },
        returnAttributes = @ReturnAttribute(
                description = "The size of the set.",
                type = {DataType.INT}),
        examples = @Example(
                syntax = "from stockStream \n" +
                        "select initSet(symbol) as initialSet \n" +
                        "insert into initStream; \n\n;" +
                        "" +
                        "from initStream#window.timeBatch(10 sec) \n" +
                        "select union(initialSet) as distinctSymbols \n" +
                        "insert into distinctStockStream; \n\n" +
                        "" +
                        "from distinctStockStream \n" +
                        "select sizeOfSet(distinctSymbols) sizeOfSymbolSet \n" +
                        "insert into sizeStream;",
                description = "The sizeStream stream will output the number of distinct stock symbols received " +
                        "during a sliding window of 10 seconds."
        )
)
public class SizeOfSetFunctionExecutor extends FunctionExecutor {

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                        SiddhiAppContext siddhiAppContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new SiddhiAppValidationException("sizeOfSet() function has to have exactly 1 parameter, currently " +
                    attributeExpressionExecutors.length + " parameters provided");
        }
        if (attributeExpressionExecutors[0].getReturnType() != Attribute.Type.OBJECT) {
            throw new OperationNotSupportedException("Parameter given for sizeOfSet() function has to be of type " +
                    "object, but found: " + attributeExpressionExecutors[0].getReturnType());
        }
    }

    /**
     * return maximum of arbitrary long set of Double values
     *
     * @param data array of Double values
     * @return max
     */
    @Override
    protected Object execute(Object[] data) {
        return null; //Since the sizeOfSet function takes in only 1 parameter, this method does not get called.
        // Hence, not implemented.
    }

    @Override
    protected Object execute(Object data) {
        if (data == null) {
            return 0;
        }
        if (!(data instanceof Set)) {
            throw new SiddhiAppRuntimeException("Input to sizeOfSet() function should be an instance of " +
                    "java.util.Set, but found " + data.getClass().getCanonicalName());
        }
        Set set = (Set) data;
        return set.size();
    }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.INT;
    }

    @Override
    public Map<String, Object> currentState() {
        return null;    //no state is maintained.
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        //Does nothing as no state is maintained.
    }
}
