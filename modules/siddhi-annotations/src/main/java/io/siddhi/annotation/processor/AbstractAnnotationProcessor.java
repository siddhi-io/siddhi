/*
 * Copyright (c)  2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.annotation.processor;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ParameterOverload;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.SystemParameter;
import io.siddhi.annotation.util.AnnotationValidationException;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Parent class for extension annotation validation processors.
 * CORE_PACKAGE_PATTERN: regex pattern for check the core package name while validation.
 * PARAMETER_NAME_PATTERN: regex pattern for check the @Extension / @Parameters / name format validation.
 * extensionClassFullName: holds the extension class full name.
 * CAMEL_CASE_PATTERN: regex pattern for check the camelCase naming convention.
 */
public class AbstractAnnotationProcessor {

    protected static final Pattern CORE_PACKAGE_PATTERN = Pattern.compile("^io.siddhi.core.");
    protected static final Pattern PARAMETER_NAME_PATTERN = Pattern.compile("^[a-z][a-z0-9]*(\\.[a-z][a-z0-9]*)*$");
    protected static final String REPETITIVE_PARAMETER_NOTATION = "...";
    protected static final Pattern CAMEL_CASE_PATTERN = Pattern.compile("^(([a-z][a-z0-9]+)([A-Z]{0,1}[a-z0-9]*)*)$");
    protected String extensionClassFullName;

    public AbstractAnnotationProcessor(String extensionClassFullName) {

        this.extensionClassFullName = extensionClassFullName;
    }

    /**
     * Basic @Extension annotation elements validation.
     *
     * @param name        name of the @Extension which needs to be validate.
     * @param description description of the @Extension  which needs to be validate.
     * @param namespace   namespace of the @Extension  which needs to be validate.
     * @throws AnnotationValidationException whenever if the validate rule violate, throws the annotation validate
     *                                       exception with proper message.
     */
    public void basicParameterValidation(String name, String description, String namespace)
            throws AnnotationValidationException {
        //Check if the @Extension name is empty.
        if (name.isEmpty()) {
            throw new AnnotationValidationException(MessageFormat.format("The @Extension -> name " +
                    " annotated in class {0} is null or empty.", extensionClassFullName));
        }
        //Check if the @Extension description is empty.
        if (description.isEmpty()) {
            throw new AnnotationValidationException(MessageFormat.format("The @Extension -> description " +
                    "annotated in class {0} is null or empty.", extensionClassFullName));
        }
        //Check if the @Extension namespace is empty.
        if (namespace.isEmpty()) {
            //The namespace cannot be null or empty if @Extension is not in core package.
            //Extract core package name by using CORE_PACKAGE_PATTERN pattern.
            if (!CORE_PACKAGE_PATTERN.matcher(extensionClassFullName).find()) {
                throw new AnnotationValidationException(MessageFormat.format("The @Extension -> namespace " +
                        "annotated in class {0} is null or empty.", extensionClassFullName));
            }
        }
    }

    /**
     * This method uses for validate @Extension / @Parameter element.
     *
     * @param parameters parameter array which needs to be validate.
     * @throws AnnotationValidationException whenever if the validate rule violate, throws the annotation validate
     *                                       exception with proper message.
     */
    public void parameterValidation(Parameter[] parameters) throws AnnotationValidationException {

        for (Parameter parameter : parameters) {
            String parameterName = parameter.name();
            //Check if the @Parameter name is empty.
            if (parameterName.isEmpty()) {
                throw new AnnotationValidationException(MessageFormat.format("The @Extension -> @Parameter -> " +
                        "name annotated in class {0} is null or empty.", extensionClassFullName));
            } else if (!(PARAMETER_NAME_PATTERN.matcher(parameterName).find() ||
                    REPETITIVE_PARAMETER_NOTATION.equals(parameterName))) {
                //Check if the @Parameter name is in a correct format 'abc.def.ghi' using regex pattern.
                throw new AnnotationValidationException(MessageFormat.format("The @Extension -> @Parameter -> " +
                                "name {0} annotated in class {1} is not in proper format 'abc.def.ghi'.",
                        parameterName, extensionClassFullName));
            }
            //Check if the @Parameter description is empty.
            if (parameter.description().isEmpty()) {
                throw new AnnotationValidationException(MessageFormat.format("The @Extension -> @Parameter -> " +
                                "name:{0} -> description annotated in class {1} is null or empty.", parameterName,
                        extensionClassFullName));
            }
            //Check if the @Parameter type is empty.
            if (parameter.type().length == 0) {
                throw new AnnotationValidationException(MessageFormat.format("The @Extension -> @Parameter -> " +
                                "name:{0} -> type annotated in class {1} is null or empty.", parameterName,
                        extensionClassFullName));
            }
            if (parameter.optional()) {
                if (parameter.defaultValue().isEmpty()) {
                    throw new AnnotationValidationException(MessageFormat.format("The @Extension -> @Parameter -> " +
                            "name:{0} -> defaultValue annotated in class {1} cannot be null or empty for the " +
                            "optional parameter.", parameterName, extensionClassFullName));
                }
            }
        }
    }

    /**
     * This method uses for validate @Extension / @ParameterOverload element.
     *
     * @param parameterOverloads parameter array which needs to be validate.
     * @param parameters         the set of supported parameters
     * @throws AnnotationValidationException whenever if the validate rule violate, throws the annotation validate
     *                                       exception with proper message.
     */
    public void parameterOverloadValidation(ParameterOverload[] parameterOverloads, Parameter[] parameters)
            throws AnnotationValidationException {

        Map<String, Parameter> parameterMap = new HashMap<>();
        Set<String> mandatoryParameterSet = new HashSet<>();
        for (Parameter parameter : parameters) {
            parameterMap.put(parameter.name(), parameter);
            if (!parameter.optional()) {
                mandatoryParameterSet.add(parameter.name());
            }
        }

        for (ParameterOverload parameterOverload : parameterOverloads) {
            String[] overloadParameterNames = parameterOverload.parameterNames();
            for (String overloadParameterName : overloadParameterNames) {
                //Check if the @Parameter name is empty.
                if (overloadParameterName.isEmpty()) {
                    throw new AnnotationValidationException(MessageFormat.format("The @Extension -> " +
                                    "@ParameterOverload -> parameterNames annotated in class {0} is null or empty.",
                            extensionClassFullName));
                } else if (!(PARAMETER_NAME_PATTERN.matcher(overloadParameterName).find() ||
                        REPETITIVE_PARAMETER_NOTATION.equals(overloadParameterName))) {
                    //Check if the @Parameter name is in a correct format 'abc.def.ghi' using regex pattern.
                    throw new AnnotationValidationException(MessageFormat.format("The @Extension -> " +
                            "@ParameterOverload -> parameterNames {0} annotated in class {1} is not " +
                            "in proper format 'abc.def.ghi'.", overloadParameterName, extensionClassFullName));
                }
                if (!(parameterMap.containsKey(overloadParameterName) ||
                        REPETITIVE_PARAMETER_NOTATION.equals(overloadParameterName))) {
                    throw new AnnotationValidationException(MessageFormat.format("The @Extension -> " +
                            "@ParameterOverload -> parameterNames {0} annotated in class {1} is not defined in " +
                            "@Extension -> @Parameter.", overloadParameterName, extensionClassFullName));
                }
            }
        }

        if (parameterOverloads.length > 0) {
            Set<String> mandatoryParameterSetViaOverload = new HashSet<>(parameterMap.keySet());
            for (Iterator<String> iterator = mandatoryParameterSetViaOverload.iterator(); iterator.hasNext(); ) {
                String parameter = iterator.next();
                for (ParameterOverload parameterOverload : parameterOverloads) {
                    boolean contains = false;
                    for (String parameterName : parameterOverload.parameterNames()) {
                        if (parameter.equalsIgnoreCase(parameterName)) {
                            contains = true;
                            break;
                        }
                    }
                    if (!contains) {
                        iterator.remove();
                        break;
                    }
                }
            }

            if (!mandatoryParameterSetViaOverload.equals(mandatoryParameterSet)) {
                throw new AnnotationValidationException("Mandatory parameter information in ParameterOverload " +
                        "and based on 'optional' annotation is a mismatch. The parameters '" +
                        mandatoryParameterSetViaOverload + "' always appearing in ParameterOverload, but '" +
                        mandatoryParameterSet + "' are defined as not 'optional' in the annotations.");
            }
        }
    }

    /**
     * This method uses for validate @Extension / @ReturnAttribute elements.
     *
     * @param returnAttributes returnA attributes array which needs to be validate.
     * @throws AnnotationValidationException whenever if the validate rule violate, throws the annotation validate
     *                                       exception with proper message.
     */
    public void returnAttributesValidation(ReturnAttribute[] returnAttributes) throws AnnotationValidationException {

        for (ReturnAttribute returnAttribute : returnAttributes) {
            String returnAttributeName = returnAttribute.name();
            //Check if the @ReturnAttributes name is empty.
            if (returnAttributeName.isEmpty()) {
                throw new AnnotationValidationException(MessageFormat.format("The @Extension -> " +
                        "@ReturnAttribute -> name annotated in class {0} is null or empty.", extensionClassFullName));
            } else if (!CAMEL_CASE_PATTERN.matcher(returnAttributeName).find()) {
                //Check if the @Extension -> @ReturnAttribute -> name is in a correct camelCase
                // format using regex pattern.
                throw new AnnotationValidationException(MessageFormat.format("The @Extension -> " +
                                "@ReturnAttribute -> name {0} annotated in class {1} is not in camelCase format.",
                        returnAttributeName, extensionClassFullName));
            }
            //Check if the @ReturnAttributes description is empty.
            if (returnAttribute.description().isEmpty()) {
                throw new AnnotationValidationException(MessageFormat.format("The @Extension -> " +
                                "@ReturnAttribute -> name:{0} -> description annotated in class {1} is null or empty.",
                        returnAttributeName, extensionClassFullName));
            }
            //Check if the @ReturnAttributes type is empty.
            if (returnAttribute.type().length == 0) {
                throw new AnnotationValidationException(MessageFormat.format("The @Extension -> " +
                                "@ReturnAttribute -> name:{0} -> type annotated in class {1} is null or empty.",
                        returnAttributeName, extensionClassFullName));
            }
        }
    }

    /**
     * This method uses for validate @Extension / @SystemParameter elements.
     *
     * @param systemParameters system property array which needs to be validate.
     * @throws AnnotationValidationException whenever if the validate rule violate, throws the annotation validate
     *                                       exception with proper message.
     */
    public void systemParametersValidation(SystemParameter[] systemParameters) throws AnnotationValidationException {
        // Iterate over all @SystemParameter annotated elements.
        for (SystemParameter systemParameter : systemParameters) {
            String systemParameterName = systemParameter.name();
            //Check if the @SystemParameter name is empty.
            if (systemParameterName.isEmpty()) {
                throw new AnnotationValidationException(MessageFormat.format("The @Extension -> " +
                        "@SystemParameter -> name annotated in class {0} is null or empty.", extensionClassFullName));
            }
            //Check if the @SystemParameter description is empty.
            if (systemParameter.description().isEmpty()) {
                throw new AnnotationValidationException(MessageFormat.format("The @Extension -> " +
                                "@SystemParameter -> name:{0} -> description annotated in class {1} is null or empty.",
                        systemParameterName, extensionClassFullName));
            }
            //Check if the @SystemParameter defaultValue is empty.
            if (systemParameter.defaultValue().isEmpty()) {
                throw new AnnotationValidationException(MessageFormat.format("The @Extension -> " +
                                "@SystemParameter -> name:{0} -> defaultValue annotated in class {1} is null or empty.",
                        systemParameterName, extensionClassFullName));
            }
            //Check if the @SystemParameter possibleParameters is empty.
            if (systemParameter.possibleParameters().length == 0) {
                throw new AnnotationValidationException(MessageFormat.format("The @Extension -> " +
                        "@SystemParameter -> name:{0} -> possibleParameters annotated in class {1} is " +
                        "null or empty.", systemParameterName, extensionClassFullName));
            }
        }
    }

    /**
     * This method uses for validate @Extension / @Example elements.
     *
     * @param examples examples array which needs to be validate.
     * @throws AnnotationValidationException whenever if the validate rule violate, throws the annotation validate
     *                                       exception with proper message.
     */
    public void examplesValidation(Example[] examples) throws AnnotationValidationException {
        //Check if the @Example annotated in all the @Extension classes.
        if (examples.length == 0) {
            throw new AnnotationValidationException(MessageFormat.format("The @Extension -> @Example " +
                    "annotated in class {0} is null or empty.", extensionClassFullName));
        } else {
            for (Example example : examples) {
                //Check if the @Example syntax is empty.
                if (example.syntax().isEmpty()) {
                    throw new AnnotationValidationException(MessageFormat.format("The @Extension -> " +
                            "@Example -> syntax annotated in class {0} is null or empty.", extensionClassFullName));
                }
                //Check if the @Example description is empty.
                if (example.description().isEmpty()) {
                    throw new AnnotationValidationException(MessageFormat.format("The @Extension -> " +
                                    "@Example -> description annotated in class {0} is null or empty.",
                            extensionClassFullName));
                }
            }
        }
    }
}
