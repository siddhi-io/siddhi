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

package io.siddhi.core.query.processor.stream.function;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ParameterOverload;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.Arrays;
import java.util.List;

/**
 * Created on 1/26/15.
 */
@Extension(
        name = "pol2Cart",
        namespace = "",
        description = "The pol2Cart function calculating the cartesian coordinates x & y for the given theta, rho " +
                "coordinates and adding them as new attributes to the existing events.",
        parameters = {
                @Parameter(name = "theta",
                        description = "The theta value of the coordinates.",
                        type = {DataType.DOUBLE},
                        dynamic = true),
                @Parameter(name = "rho",
                        description = "The rho value of the coordinates.",
                        type = {DataType.DOUBLE},
                        dynamic = true),
                @Parameter(name = "z",
                        description = "z value of the cartesian coordinates.",
                        type = {DataType.DOUBLE},
                        optional = true,
                        dynamic = true,
                        defaultValue = "If z value is not given, drop the third parameter of the output.")
        },
        parameterOverloads = {
                @ParameterOverload(parameterNames = {"theta", "rho"}),
                @ParameterOverload(parameterNames = {"theta", "rho", "z"})
        },
        examples = {
                @Example(
                        syntax = "from PolarStream#pol2Cart(theta, rho)\n" +
                                "select x, y \n" +
                                "insert into outputStream ;",
                        description = "This will return cartesian coordinates (4.99953024681082, 0.06853693328228748)" +
                                " for theta: 0.7854 and rho: 5."),
                @Example(
                        syntax = "from PolarStream#pol2Cart(theta, rho, 3.4)\n" +
                                "select x, y, z \n" +
                                "insert into outputStream ;",
                        description = "This will return cartesian coordinates (4.99953024681082, 0.06853693328228748," +
                                " 3.4)for theta: 0.7854 and rho: 5 and z: 3.4.")
        }
)
public class Pol2CartStreamFunctionProcessor extends StreamFunctionProcessor {


    private int inputExecutorLength;
    private List<Attribute> retrunAttributes;

    /**
     * The init method of the StreamFunction
     *
     * @param inputDefinition              the incoming stream definition
     * @param attributeExpressionExecutors the executors for the function parameters
     * @param siddhiQueryContext           siddhi query context
     * @return the additional output attributes introduced by the function
     */
    @Override
    protected StateFactory init(AbstractDefinition inputDefinition,
                                ExpressionExecutor[] attributeExpressionExecutors,
                                ConfigReader configReader, boolean outputExpectsExpiredEvents,
                                SiddhiQueryContext siddhiQueryContext) {
        inputExecutorLength = attributeExpressionExecutors.length;

        if (inputExecutorLength < 2 || inputExecutorLength > 3) {
            throw new SiddhiAppValidationException("Input parameters for poleCart can either be 'theta,rho' or " +
                    "'theta,rho,z', but " +
                    attributeExpressionExecutors.length + " attributes found");
        }
        for (int i = 0; i < inputExecutorLength; i++) {
            ExpressionExecutor expressionExecutor = attributeExpressionExecutors[i];
            if (expressionExecutor.getReturnType() != Attribute.Type.DOUBLE) {
                throw new SiddhiAppValidationException("Input attribute " + i + " is expected to return Double, " +
                        "but its returning " + expressionExecutor.getReturnType());
            }

        }
        if (attributeExpressionExecutors.length == 2) {
            for (Attribute attribute : inputDefinition.getAttributeList()) {
                if (attribute.getName().equals("x") || attribute.getName().equals("y")) {
                    throw new SiddhiAppValidationException("Input stream " + inputDefinition.getId() + " should " +
                            "not contain attributes with name 'x' or 'y', but found " + attribute);
                }
            }
            retrunAttributes = Arrays.asList(
                    new Attribute("x", Attribute.Type.DOUBLE),
                    new Attribute("y", Attribute.Type.DOUBLE));
        } else {
            for (Attribute attribute : inputDefinition.getAttributeList()) {
                if (attribute.getName().equals("x") || attribute.getName().equals("y") || attribute.getName().equals
                        ("z")) {
                    throw new SiddhiAppValidationException("Input stream " + inputDefinition.getId() + " should " +
                            "not contain attributes with name 'x' or 'y' or 'z', but found " + attribute);
                }
            }
            retrunAttributes = Arrays.asList(
                    new Attribute("x", Attribute.Type.DOUBLE),
                    new Attribute("y", Attribute.Type.DOUBLE),
                    new Attribute("z", Attribute.Type.DOUBLE));
        }
        return null;
    }

    /**
     * The process method of the StreamFunction, used when multiple function parameters are provided
     *
     * @param data the data values for the function parameters
     * @return the date for additional output attributes introduced by the function
     */
    @Override
    protected Object[] process(Object[] data) {

        double theta = (Double) data[0];
        double rho = (Double) data[1];
        Object[] output = new Object[inputExecutorLength];
        output[0] = rho * Math.cos(Math.toRadians(theta));
        output[1] = rho * Math.sin(Math.toRadians(theta));
        if (inputExecutorLength == 3) {
            output[2] = data[2];
        }
        return output;
    }

    /**
     * The process method of the StreamFunction, used when single function parameter is provided
     *
     * @param data the data value for the function parameter
     * @return the date for additional output attribute introduced by the function
     */
    @Override
    protected Object[] process(Object data) {
        throw new IllegalStateException("pol2Cart cannot execute for single data " + data);
    }

    @Override
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        //Do nothing
    }

    @Override
    public List<Attribute> getReturnAttributes() {
        return retrunAttributes;
    }
}
