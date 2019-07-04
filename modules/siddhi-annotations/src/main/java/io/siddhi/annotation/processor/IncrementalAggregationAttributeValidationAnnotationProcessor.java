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

import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ReturnAttribute;
import io.siddhi.annotation.util.AnnotationValidationException;

import java.text.MessageFormat;

/**
 * This processor will extend the validation rules for validating Incremental Aggregation Attribute specific
 * annotation contents.
 */
public class IncrementalAggregationAttributeValidationAnnotationProcessor extends AbstractAnnotationProcessor {
    public IncrementalAggregationAttributeValidationAnnotationProcessor(String extensionClassFullName) {
        super(extensionClassFullName);
    }

    @Override
    public void parameterValidation(Parameter[] parameters) throws AnnotationValidationException {
        for (Parameter parameter : parameters) {
            String parameterName = parameter.name();
            //Check if the @Parameter name is empty.
            if (parameterName.isEmpty()) {
                throw new AnnotationValidationException(MessageFormat.format("The @Extension -> @Parameter " +
                        "-> name annotated in class {0} is null or empty.", extensionClassFullName));
            } else if (!(PARAMETER_NAME_PATTERN.matcher(parameterName).find() ||
                    REPETITIVE_PARAMETER_NOTATION.equals(parameterName))) {
                //Check if the @Parameter name is in a correct format 'abc.def.ghi' using regex pattern.
                throw new AnnotationValidationException(MessageFormat.format("The @Extension -> @Parameter " +
                                "-> name:{0} annotated in class {1} is not in proper format ''abc.def.ghi''.",
                        parameterName, extensionClassFullName));
            }
            //Check if the @Parameter description is empty.
            if (parameter.description().isEmpty()) {
                throw new AnnotationValidationException(MessageFormat.format("The @Extension -> @Parameter ->" +
                                " name:{0} -> description annotated in class {1} is null or empty.", parameterName,
                        extensionClassFullName));
            }
            //Check if the @Parameter type is empty.
            if (parameter.type().length == 0) {
                throw new AnnotationValidationException(MessageFormat.format("The @Extension -> @Parameter ->" +
                                " name:{0} -> type annotated in class {1} is null or empty.", parameterName,
                        extensionClassFullName));
            }
        }
    }

    @Override
    public void returnAttributesValidation(ReturnAttribute[] returnAttributes) throws AnnotationValidationException {
        if (returnAttributes.length == 0) {
            //Throw error if the @ReturnAttributes empty.
            throw new AnnotationValidationException(MessageFormat.format("The @Extension -> " +
                    "@ReturnAttribute annotated in class {0} is null or empty.", extensionClassFullName));
        } else if (returnAttributes.length == 1) {
            String returnAttributeName = returnAttributes[0].name();
            //Check if the @ReturnAttributes name is empty.
            if (!returnAttributeName.isEmpty()) {
                throw new AnnotationValidationException(MessageFormat.format("The @Extension -> " +
                        "@ReturnAttribute -> name cannot be annotated in class {1}.", extensionClassFullName));
            }
            //Check if the @ReturnAttributes description is empty.
            if (returnAttributes[0].description().isEmpty()) {
                throw new AnnotationValidationException(MessageFormat.format("The @Extension -> " +
                                "@ReturnAttribute -> name:{0} -> description annotated in class {1} is null or empty.",
                        returnAttributeName, extensionClassFullName));
            }
            //Check if the @ReturnAttributes type is empty.
            if (returnAttributes[0].type().length == 0) {
                throw new AnnotationValidationException(MessageFormat.format("The @Extension ->" +
                                " @ReturnAttribute -> name{0} -> type annotated in class {1} is null or empty.",
                        returnAttributeName, extensionClassFullName));
            }
        } else {
            //Throw error if the @ReturnAttributes count is more than one.
            throw new AnnotationValidationException(MessageFormat.format("Only one @Extension -> " +
                    "@ReturnAttribute can be annotated in class {0}.", extensionClassFullName));
        }
    }
}
