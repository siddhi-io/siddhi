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
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ParameterOverload;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;

/**
 * Executor class for Siddhi cast function. Converts the given parameter according to the castTo parameter.
 * Incompatible arguments cause {@link ClassCastException} if further processed.
 */
@Extension(
        name = "cast",
        namespace = "",
        description = "Converts the first parameter according to the cast.to parameter. Incompatible arguments cause " +
                "Class Cast exceptions if further processed. This function is used with map extension that returns " +
                "attributes of the object type. You can use this function to cast the object to an accurate and " +
                "concrete type.",
        parameters = {
                @Parameter(name = "to.be.caster",
                        description = "This specifies the attribute to be casted.",
                        type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT,
                                DataType.STRING, DataType.BOOL, DataType.OBJECT},
                        dynamic = true),
                @Parameter(name = "cast.to",
                        description = "A string constant parameter expressing the cast to type using one of the " +
                                "following strings values: int, long, float, double, string, bool.",
                        type = {DataType.STRING},
                        dynamic = true)
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"to.be.caster", "cast.to"})
        },
        returnAttributes = @ReturnAttribute(
                description = "Returned type will be defined by the cast.to string constant value.",
                type = {DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.FLOAT,
                        DataType.STRING, DataType.BOOL, DataType.OBJECT}),
        examples = {
                @Example(
                        syntax = "from fooStream\n" +
                                "select symbol as name, cast(temp, 'double') as temp\n" +
                                "insert into barStream;",
                        description = "This will cast the fooStream temp field value into 'double' format.")
        }
)
public class CastFunctionExecutor extends FunctionExecutor {
    private Attribute.Type returnType = Attribute.Type.OBJECT;

    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length != 2) {
            throw new SiddhiAppValidationException("Invalid no of arguments passed to common:cast() function, " +
                    "required 2 parameters, but found " +
                    attributeExpressionExecutors.length);
        }
        if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
            throw new SiddhiAppValidationException("The second argument has to be a string constant specifying " +
                    "one of the supported data types "
                    + "(int, long, float, double, string, bool)");
        } else {
            String type = attributeExpressionExecutors[1].execute(null).toString();
            if (type.toLowerCase().equals("int")) {
                returnType = Attribute.Type.INT;
            } else if (type.toLowerCase().equals("long")) {
                returnType = Attribute.Type.LONG;
            } else if (type.toLowerCase().equals("float")) {
                returnType = Attribute.Type.FLOAT;
            } else if (type.toLowerCase().equals("double")) {
                returnType = Attribute.Type.DOUBLE;
            } else if (type.toLowerCase().equals("bool")) {
                returnType = Attribute.Type.BOOL;
            } else if (type.toLowerCase().equals("string")) {
                returnType = Attribute.Type.STRING;
            } else {
                throw new SiddhiAppValidationException("Type must be one of int, long, float, double, bool, " +
                        "string");
            }
        }
        return null;
    }

    @Override
    protected Object execute(Object[] data, State state) {
        if (returnType == Attribute.Type.LONG && data[0] instanceof Integer) {
            return ((Integer) data[0]).longValue();
        }
        return data[0];
    }

    @Override
    protected Object execute(Object data, State state) {
        return null;
    }

    @Override
    public Attribute.Type getReturnType() {
        return returnType;
    }

}


