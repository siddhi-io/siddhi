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

package io.siddhi.query.api.util;

import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.annotation.Element;
import io.siddhi.query.api.exception.DuplicateAnnotationException;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Helper class to extract annotations
 */
public class AnnotationHelper {

    public static Annotation getAnnotation(String[] annotationNames, List<Annotation> annotationList) {

        if (annotationNames.length == 1) {
            return getAnnotation(annotationNames[0], annotationList);
        } else {
            return getAnnotation(Arrays.copyOfRange(annotationNames, 1, annotationNames.length),
                    getAnnotation(annotationNames[0], annotationList).getAnnotations()
            );
        }
    }

    public static Annotation getAnnotation(String annotationName, List<Annotation> annotationList) {

        Annotation annotation = null;
        for (Annotation aAnnotation : annotationList) {
            if (annotationName.equalsIgnoreCase(aAnnotation.getName())) {
                if (annotation == null) {
                    annotation = aAnnotation;
                } else {
                    throw new DuplicateAnnotationException("Annotation @" + annotationName + " is defined twice",
                            aAnnotation.getQueryContextStartIndex(), aAnnotation.getQueryContextEndIndex());
                }
            }
        }
        return annotation;
    }

    public static List<Annotation> getAnnotations(String annotationName, List<Annotation> annotationList) {

        List<Annotation> annotations = new LinkedList<>();
        for (Annotation aAnnotation : annotationList) {
            if (annotationName.equalsIgnoreCase(aAnnotation.getName())) {
                annotations.add(aAnnotation);
            }
        }
        return annotations;
    }

    // TODO: 1/28/17 update helper methods to work with nested annotations.
    public static Element getAnnotationElement(String annotationName, String elementName,
                                               List<Annotation> annotationList) {

        Annotation annotation = getAnnotation(annotationName, annotationList);
        if (annotation != null) {
            Element element = null;
            for (Element aElement : annotation.getElements()) {
                if (elementName == null) {
                    if (aElement.getKey() == null) {

                        if (element == null) {
                            element = aElement;
                        } else {
                            throw new DuplicateAnnotationException("Annotation element @" + annotationName + "(...) " +
                                    "is defined twice", aElement.getQueryContextStartIndex(),
                                    aElement.getQueryContextEndIndex());
                        }
                    }
                } else {
                    if (elementName.equalsIgnoreCase(aElement.getKey())) {

                        if (element == null) {
                            element = aElement;
                        } else {
                            throw new DuplicateAnnotationException("Annotation element @" + annotationName + "(" +
                                    elementName + "=...) is defined twice", aElement.getQueryContextStartIndex(),
                                    aElement.getQueryContextEndIndex());
                        }
                    }
                }

            }
            return element;
        }
        return null;
    }
}
