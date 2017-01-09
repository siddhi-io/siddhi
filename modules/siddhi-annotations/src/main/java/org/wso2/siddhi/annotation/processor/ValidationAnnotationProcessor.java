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
package org.wso2.siddhi.annotation.processor;

import org.wso2.siddhi.annotation.AdditionalAttribute;
import org.wso2.siddhi.annotation.Description;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.Parameters;
import org.wso2.siddhi.annotation.Return;
import org.wso2.siddhi.annotation.ReturnEvent;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.tools.Diagnostic;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Annotation processor for validating the siddhi annotations added for documenting the processors at compile time
 */
public class ValidationAnnotationProcessor extends AbstractProcessor {
    private static final String STREAM_PROCESSOR_SUPER_CLASS = "org.wso2.siddhi.core.query.processor.stream.StreamProcessor";
    private static final String FUNCTION_EXECUTOR_SUPER_CLASS = "org.wso2.siddhi.core.executor.function.FunctionExecutor";
    private static final String ATTRIBUTE_AGGREGATOR_SUPER_CLASS = "org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator";

    private List<Class<? extends Annotation>> annotationsClasses;    // annotation classes that will be validated by this processor
    private Messager messager;

    @Override
    public synchronized void init(ProcessingEnvironment env) {
        super.init(env);
        messager = env.getMessager();

        // Populating the supported annotations class
        annotationsClasses = new ArrayList<>();
        annotationsClasses.add(AdditionalAttribute.class);
        annotationsClasses.add(Description.class);
        annotationsClasses.add(Example.class);
        annotationsClasses.add(Parameter.class);
        annotationsClasses.add(Parameters.class);
        annotationsClasses.add(Return.class);
        annotationsClasses.add(ReturnEvent.class);
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        // Looping the annotation classes in the annotation classes list
        for (Class<? extends Annotation> annotationClass : annotationsClasses) {
            // Looping the elements annotated with the annotation classes
            for (Element element : roundEnv.getElementsAnnotatedWith(annotationClass)) {
                if (element.getKind() == ElementKind.CLASS) {
                    // Throw error if @AdditionalAttribute or @ReturnEvent is applied to any class not extending StreamProcessor
                    if (annotationClass.equals(ReturnEvent.class)) {
                        validateSuperClassInheritance(
                                element, new String[]{STREAM_PROCESSOR_SUPER_CLASS}
                        );
                    }

                    // Throw error if @AdditionalAttribute is directly applied to classes
                    if (annotationClass.equals(AdditionalAttribute.class)) {
                        showBuildError(
                                element, "%s should not be directly annotated with %s. Use %s instead.",
                                element.getSimpleName(),
                                AdditionalAttribute.class.getCanonicalName(),
                                ReturnEvent.class.getCanonicalName()
                        );
                    }

                    // Throw error if @Return is applied to classes extending StreamProcessor, WindowProcessor & StreamFunction
                    if (annotationClass.equals(Return.class)) {
                        validateSuperClassInheritance(
                                element, new String[]{
                                        FUNCTION_EXECUTOR_SUPER_CLASS, ATTRIBUTE_AGGREGATOR_SUPER_CLASS
                                }
                        );
                    }
                } else {
                    showBuildError(
                            element, "Only classes can be annotated with @%s",
                            annotationClass.getCanonicalName()
                    );
                }
            }
        }
        return false;   // Returning false since this processor only validates
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        HashSet<String> annotationTypes = new HashSet<>();
        for (Class<? extends Annotation> annotationClass : annotationsClasses) {
            annotationTypes.add(annotationClass.getCanonicalName());
        }
        return annotationTypes;
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.RELEASE_8;
    }

    /**
     * Show build showBuildError
     *
     * @param element Element for which the showBuildError should be shown
     * @param message The showBuildError message to be shown. This string will be formatted using the args
     * @param args    The arguments for formatting the message string
     */
    private void showBuildError(Element element, String message, Object... args) {
        messager.printMessage(Diagnostic.Kind.ERROR, String.format(message, args), element);
    }

    /**
     * Validate if the element specified is inherited from the super class specified
     * This will throw an error if it is not extnded from it
     *
     * @param elementToValidate The element to validate if extended from one of the superclasses
     * @param superClassNames   The super classes one of which the element should inherit
     */
    private void validateSuperClassInheritance(Element elementToValidate, String[] superClassNames) {
        TypeMirror superType = ((TypeElement) elementToValidate).getSuperclass();
        boolean isAChildClass = false;

        // Looping the inheritance hierarchy to check if the element inherits at least one of the super classes specified
        while (!"none".equals(superType.toString())) {
            Element superTypeElement = ((DeclaredType) superType).asElement();

            if (Arrays.asList(superClassNames).contains(superTypeElement.toString())) {
                isAChildClass = true;
                break;
            }

            superType = ((TypeElement) superTypeElement).getSuperclass();
        }

        if (!isAChildClass) {
            showBuildError(
                    elementToValidate, "%s does not extend %s",
                    elementToValidate.getSimpleName(), String.join(" or ", (CharSequence[]) superClassNames)
            );
        }
    }
}
