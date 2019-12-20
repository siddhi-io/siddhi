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

package io.siddhi.core.util.extension.validator;

import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ParameterOverload;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.parser.helper.AnnotationHelper;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import static io.siddhi.core.util.SiddhiConstants.REPETITIVE_PARAMETER_NOTATION;

/**
 * Validates the extension specific parameters of siddhi App with the patterns specified in the
 * {@literal @}ParameterOverload annotation in the extension class
 */
public class InputParameterValidator {

    /**
     * The method which validates the extension specific parameters of siddhi App with the pattern specified in the
     * {@link ParameterOverload} annotation in the extension class
     *
     * @param objectHavingAnnotation       the object which has Extension annotation
     * @param attributeExpressionExecutors the executors of each function parameters
     * @throws SiddhiAppValidationException SiddhiAppValidation exception
     */
    public static void validateExpressionExecutors(Object objectHavingAnnotation,
                                                   ExpressionExecutor[] attributeExpressionExecutors)
            throws SiddhiAppValidationException {

        Extension annotation = objectHavingAnnotation.getClass().getAnnotation(Extension.class);
        if (annotation == null) {
            return;
        }
        ParameterOverload[] parameterOverloads = annotation.parameterOverloads();
        Parameter[] parameters = annotation.parameters();
        String key = AnnotationHelper.createAnnotationKey(annotation);

        //Count the mandatory number of parameters specified in @Extension
        int mandatoryCount = 0;
        Map<String, Parameter> parameterMap = new HashMap<>();

        for (Parameter parameter : parameters) {
            if (!parameter.optional()) {
                mandatoryCount++;
            }
            parameterMap.put(parameter.name(), parameter);
        }

        //Find the parameterOverLoad
        ParameterOverload parameterOverload = null;
        for (ParameterOverload aParameterOverload : parameterOverloads) {
            String[] overloadParameterNames = aParameterOverload.parameterNames();
            if (overloadParameterNames.length == attributeExpressionExecutors.length &&
                    (overloadParameterNames.length == 0 || !overloadParameterNames[overloadParameterNames.length - 1].
                            equals(REPETITIVE_PARAMETER_NOTATION))) {
                boolean isExpectedParameterOverload = true;
                for (int i = 0; i < overloadParameterNames.length; i++) {
                    String overloadParameterName = overloadParameterNames[i];
                    Parameter parameter = parameterMap.get(overloadParameterName);
                    boolean supportedReturnType = false;
                    for (DataType type : parameter.type()) {
                        if (attributeExpressionExecutors[i].getReturnType().toString().
                                equalsIgnoreCase(type.toString())) {
                            supportedReturnType = true;
                            break;
                        }
                    }
                    if (!supportedReturnType) {
                        isExpectedParameterOverload = false;
                        break;
                    }
                }
                if (isExpectedParameterOverload) {
                    parameterOverload = aParameterOverload;
                    break;
                }
            } else if (overloadParameterNames.length - 1 <= attributeExpressionExecutors.length &&
                    overloadParameterNames.length > 0 &&
                    overloadParameterNames[overloadParameterNames.length - 1].equals(REPETITIVE_PARAMETER_NOTATION)) {
                if (attributeExpressionExecutors.length > 0) {
                    boolean isExpectedParameterOverload = true;
                    for (int i = 0; i < attributeExpressionExecutors.length; i++) {
                        Parameter parameter = null;
                        String overloadParameterName = null;
                        if (i < overloadParameterNames.length - 1) {
                            overloadParameterName = overloadParameterNames[i];
                        } else {
                            overloadParameterName = overloadParameterNames[overloadParameterNames.length - 2];
                        }
                        parameter = parameterMap.get(overloadParameterName);
                        boolean supportedReturnType = false;
                        for (DataType type : parameter.type()) {
                            if (attributeExpressionExecutors[i].getReturnType().toString().
                                    equalsIgnoreCase(type.toString())) {
                                supportedReturnType = true;
                                break;
                            }
                        }
                        if (!supportedReturnType) {
                            isExpectedParameterOverload = false;
                            break;
                        }
                    }
                    if (isExpectedParameterOverload) {
                        parameterOverload = aParameterOverload;
                        break;
                    }
                }
            }
        }

        if (parameterOverload == null) {
            if (parameterOverloads.length > 0) {
                List<Attribute.Type> returnTypes = new ArrayList<>();
                for (ExpressionExecutor expressionExecutor : attributeExpressionExecutors) {
                    returnTypes.add(expressionExecutor.getReturnType());
                }
                String formattedParamOverloadString = getSupportedParamOverloads(parameterMap, parameterOverloads);
                throw new SiddhiAppValidationException("There is no parameterOverload for '" + key +
                        "' that matches attribute types '" + returnTypes.stream()
                        .map(String::valueOf).collect(Collectors.joining(", ", "<", ">")) +
                        "'. Supported parameter overloads are " + formattedParamOverloadString + ".");
            } else {
                if (mandatoryCount > attributeExpressionExecutors.length) {
                    throw new SiddhiAppValidationException("The '" + key + "' expects at least " + mandatoryCount +
                            " parameters, but found only " + attributeExpressionExecutors.length +
                            " input parameters.");
                }

            }
        } else {
            String[] overloadParameterNames = parameterOverload.parameterNames();
            for (int i = 0; i < overloadParameterNames.length; i++) {
                String overloadParameterName = overloadParameterNames[i];
                Parameter parameter = parameterMap.get(overloadParameterName);
                if (parameter != null && !parameter.dynamic() &&
                        !(attributeExpressionExecutors[i] instanceof ConstantExpressionExecutor)) {
                    throw new SiddhiAppValidationException("The '" + key + "' expects input parameter '" +
                            parameter.name() + "' at position '" + i + "' to be static," +
                            " but found a dynamic attribute.");
                }
            }
        }
    }

    private static String getSupportedParamOverloads(Map<String, Parameter> parameterMap,
                                                     ParameterOverload[] parameterOverloads) {
        StringJoiner stringJoiner = new StringJoiner(", ");
        for (ParameterOverload parameterOverload : parameterOverloads) {
            String[] parameterNames = parameterOverload.parameterNames();
            if (parameterNames.length != 0) {
                StringJoiner paramOverloadStringJoiner = new StringJoiner(", ", "(", ")");
                for (int i = 0; i < parameterNames.length; i++) {
                    StringBuilder stringBuilder = new StringBuilder();
                    if (!SiddhiConstants.REPETITIVE_PARAMETER_NOTATION.equals(parameterNames[i])) {
                        stringBuilder.append(getFormattedStringForDataType(parameterMap.
                                get(parameterNames[i]).type()));
                        stringBuilder.append(" ").append(parameterNames[i]);
                    } else {
                        stringBuilder.append(getFormattedStringForDataType(parameterMap.
                                get(parameterNames[i - 1]).type()));
                        stringBuilder.append(" ").append(SiddhiConstants.REPETITIVE_PARAMETER_NOTATION);
                    }
                    paramOverloadStringJoiner.add(stringBuilder);
                }
                stringJoiner.add(paramOverloadStringJoiner.toString());
            }
        }
        return stringJoiner.toString();
    }

    private static String getFormattedStringForDataType(DataType[] dataTypes) {
        StringJoiner stringJoiner = new StringJoiner("|", "<", ">");
        for (DataType dataType : dataTypes) {
            stringJoiner.add(dataType.name());
        }
        return stringJoiner.toString();
    }
}

