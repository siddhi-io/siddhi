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

package io.siddhi.core.util.parser;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.event.stream.converter.ZeroStreamEventConverter;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;
import io.siddhi.core.exception.OperationNotSupportedException;
import io.siddhi.core.table.holder.EventHolder;
import io.siddhi.core.table.holder.IndexEventHolder;
import io.siddhi.core.table.holder.IndexEventHolderForCache;
import io.siddhi.core.table.holder.ListEventHolder;
import io.siddhi.core.table.holder.PrimaryKeyReferenceHolder;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.annotation.Element;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import io.siddhi.query.api.util.AnnotationHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Class to parse {@link EventHolder}
 */
public class EventHolderPasser {
    private static final Logger log = LogManager.getLogger(EventHolderPasser.class);

    public static EventHolder parse(AbstractDefinition tableDefinition, StreamEventFactory tableStreamEventFactory,
                                    SiddhiAppContext siddhiAppContext, boolean isCacheTable) {
        ZeroStreamEventConverter eventConverter = new ZeroStreamEventConverter();

        PrimaryKeyReferenceHolder[] primaryKeyReferenceHolders = null;

        Map<String, Integer> indexMetaData = new HashMap<String, Integer>();

        // primaryKey.
        Annotation primaryKeyAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_PRIMARY_KEY,
                tableDefinition.getAnnotations());
        if (primaryKeyAnnotation != null) {
            if (primaryKeyAnnotation.getElements().size() == 0) {
                throw new SiddhiAppValidationException(SiddhiConstants.ANNOTATION_PRIMARY_KEY + " annotation " +
                        "contains " + primaryKeyAnnotation.getElements().size() + " element, at '" +
                        tableDefinition.getId() + "'");
            }
            primaryKeyReferenceHolders = primaryKeyAnnotation.getElements().stream()
                    .map(element -> element.getValue().trim())
                    .map(key -> new PrimaryKeyReferenceHolder(key, tableDefinition.getAttributePosition(key)))
                    .toArray(PrimaryKeyReferenceHolder[]::new);
        }

        for (Annotation indexAnnotation : AnnotationHelper.getAnnotations(SiddhiConstants.ANNOTATION_INDEX,
                tableDefinition.getAnnotations())) {
            if (indexAnnotation.getElements().size() == 0) {
                throw new SiddhiAppValidationException(SiddhiConstants.ANNOTATION_INDEX + " annotation of " +
                        "in-memory table should contain only one index element, but found "
                        + indexAnnotation.getElements().size() + " element",
                        indexAnnotation.getQueryContextStartIndex(),
                        indexAnnotation.getQueryContextEndIndex());
            } else if (indexAnnotation.getElements().size() > 1) {
                throw new SiddhiAppValidationException(SiddhiConstants.ANNOTATION_INDEX + " annotation of the " +
                        "in-memory table should only contain one index element but found "
                        + indexAnnotation.getElements().size() + " elements. To use multiple indexes, " +
                        "define multiple '@index(<index key>)' annotations with one index element " +
                        "per each index key",
                        indexAnnotation.getQueryContextStartIndex(),
                        indexAnnotation.getQueryContextEndIndex());
            }
            for (Element element : indexAnnotation.getElements()) {
                Integer previousValue = indexMetaData.put(element.getValue().trim(), tableDefinition
                        .getAttributePosition(element.getValue().trim()));
                if (previousValue != null) {
                    throw new SiddhiAppValidationException("Multiple " + SiddhiConstants.ANNOTATION_INDEX + " " +
                            "annotations defined with same attribute '" + element.getValue().trim() + "', at '" +
                            tableDefinition.getId() + "'", indexAnnotation.getQueryContextStartIndex(),
                            indexAnnotation.getQueryContextEndIndex());
                }
            }
        }

        // not support indexBy.
        Annotation indexByAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_INDEX_BY,
                tableDefinition.getAnnotations());
        if (indexByAnnotation != null) {
            throw new OperationNotSupportedException(SiddhiConstants.ANNOTATION_INDEX_BY + " annotation is not " +
                    "supported anymore, please use @PrimaryKey or @Index annotations instead," +
                    " at '" + tableDefinition.getId() + "'");
        }

        if (primaryKeyReferenceHolders != null || indexMetaData.size() > 0) {
            boolean isNumeric = false;
            if (primaryKeyReferenceHolders != null) {
                if (primaryKeyReferenceHolders.length == 1) {
                    Attribute.Type type = tableDefinition.getAttributeType(
                            primaryKeyReferenceHolders[0].getPrimaryKeyAttribute());
                    if (type == Attribute.Type.DOUBLE || type == Attribute.Type.FLOAT || type == Attribute.Type.INT ||
                            type == Attribute.Type.LONG) {
                        isNumeric = true;
                    }
                }

            }
            if (isCacheTable) {
                return new IndexEventHolderForCache(tableStreamEventFactory, eventConverter, primaryKeyReferenceHolders, isNumeric,
                        indexMetaData, tableDefinition, siddhiAppContext);
            } else {
                return new IndexEventHolder(tableStreamEventFactory, eventConverter, primaryKeyReferenceHolders, isNumeric,
                        indexMetaData, tableDefinition, siddhiAppContext);
            }
        } else {
            MetaStreamEvent metaStreamEvent = new MetaStreamEvent();
            for (Attribute attribute : tableDefinition.getAttributeList()) {
                metaStreamEvent.addOutputData(attribute);
            }
            StreamEventCloner streamEventCloner = new StreamEventCloner(metaStreamEvent, tableStreamEventFactory);
            return new ListEventHolder(tableStreamEventFactory, eventConverter,
                    new StreamEventClonerHolder(streamEventCloner));
        }
    }


}
