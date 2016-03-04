/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.extension.eventtable;

import org.apache.axis2.transport.base.threads.NativeWorkerPool;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.MetaComplexEvent;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.StreamEventPool;
import org.wso2.siddhi.core.event.stream.converter.ZeroStreamEventConverter;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.table.EventTable;
import org.wso2.siddhi.core.util.SiddhiConstants;
import org.wso2.siddhi.core.util.collection.operator.Finder;
import org.wso2.siddhi.core.util.collection.operator.Operator;
import org.wso2.siddhi.core.util.snapshot.Snapshotable;
import org.wso2.siddhi.extension.eventtable.rdbms.RDBMSEventTableConstants;
import org.wso2.siddhi.extension.eventtable.sync.SyncOperatorParser;
import org.wso2.siddhi.extension.eventtable.sync.SyncTableHandler;
import org.wso2.siddhi.extension.eventtable.sync.util.jms.*;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.TableDefinition;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.util.AnnotationHelper;

import java.io.IOException;
import java.util.*;


/**
 * In-memory event table implementation of SiddhiQL.
 */
public class SyncEventTable implements EventTable, Snapshotable {

    private TableDefinition tableDefinition;
    private StreamEventCloner streamEventCloner;
    private StreamEventPool streamEventPool;
    private ZeroStreamEventConverter eventConverter = new ZeroStreamEventConverter();
    private List<StreamEvent> eventsList;
    private String elementId;
    // For indexed table.
    private String indexAttribute = null;
    private SyncTableHandler syncTableHandler;
    private int minThreadPoolSize = 4;
    private int maxThreadPoolSize = 4;
    private int KeepAliveTimeInMillis = 1000;
    private int jobQueueSize = 1000;

    private int indexPosition;
    private SortedMap<Object, StreamEvent> eventsMap;
    private Logger log = Logger.getLogger(SyncEventTable.class);

    @Override
    public void init(TableDefinition tableDefinition, ExecutionPlanContext executionPlanContext) {
        if (elementId == null) {
            elementId = executionPlanContext.getElementIdGenerator().createNewId();
        }
        executionPlanContext.getSnapshotService().addSnapshotable(this);
        SyncEventTable syncEventTable = this;

        this.tableDefinition = tableDefinition;
        int bloomFilterSize = RDBMSEventTableConstants.BLOOM_FILTER_SIZE;
        int bloomFilterHashFunctions = RDBMSEventTableConstants.BLOOM_FILTER_HASH_FUNCTIONS;

        Annotation fromAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_FROM,
                tableDefinition.getAnnotations());

        MetaStreamEvent metaStreamEvent = new MetaStreamEvent();
        metaStreamEvent.addInputDefinition(tableDefinition);
        for (Attribute attribute : tableDefinition.getAttributeList()) {
            metaStreamEvent.addOutputData(attribute);
        }

        //Bloom Filter
        String bloomsEnabled = fromAnnotation.getElement(RDBMSEventTableConstants.ANNOTATION_ELEMENT_BLOOM_FILTERS);

        // Adding indexes.
        Annotation annotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_INDEX_BY,
                tableDefinition.getAnnotations());
        if (annotation != null) {
            if (annotation.getElements().size() > 1) {
                throw new OperationNotSupportedException(SiddhiConstants.ANNOTATION_INDEX_BY + " annotation contains " +
                        annotation.getElements().size() +
                        " elements, Siddhi Sync table only supports indexing based on a single attribute");
            }
            if (annotation.getElements().size() == 0) {
                throw new ExecutionPlanValidationException(SiddhiConstants.ANNOTATION_INDEX_BY + " annotation contains "
                        + annotation.getElements().size() + " element");
            }
            indexAttribute = annotation.getElements().get(0).getValue();
            indexPosition = tableDefinition.getAttributePosition(indexAttribute);
            eventsMap = new TreeMap<Object, StreamEvent>();
        } else {
            eventsList = new LinkedList<StreamEvent>();
        }

        this.syncTableHandler = new SyncTableHandler(tableDefinition);


        //Bloom Filter Implementation
        if (bloomsEnabled != null && bloomsEnabled.equalsIgnoreCase("enable")) {
            String bloomsFilterSize = fromAnnotation.getElement(RDBMSEventTableConstants.ANNOTATION_ELEMENT_BLOOM_FILTERS_SIZE);
            String bloomsFilterHash = fromAnnotation.getElement(RDBMSEventTableConstants.ANNOTATION_ELEMENT_BLOOM_FILTERS_HASH);
            if (bloomsFilterSize != null) {
                bloomFilterSize = Integer.parseInt(bloomsFilterSize);
            }
            if (bloomsFilterHash != null) {
                bloomFilterHashFunctions = Integer.parseInt(bloomsFilterHash);
            }

            syncTableHandler.setBloomFilterProperties(bloomFilterSize, bloomFilterHashFunctions);
            syncTableHandler.buildBloomFilters(syncEventTable);
        }

        subscribeForJmsEvents();
        streamEventPool = new StreamEventPool(metaStreamEvent, 10);
        streamEventCloner = new StreamEventCloner(metaStreamEvent, streamEventPool);
    }

    @Override
    public TableDefinition getTableDefinition() {
        return tableDefinition;
    }

    @Override
    public synchronized void add(ComplexEventChunk addingEventChunk) {
        addingEventChunk.reset();
        while (addingEventChunk.hasNext()) {
            ComplexEvent complexEvent = addingEventChunk.next();
            StreamEvent streamEvent = streamEventPool.borrowEvent();
            eventConverter.convertStreamEvent(complexEvent, streamEvent);
            if (indexAttribute != null) {
                eventsMap.put(streamEvent.getOutputData()[indexPosition], streamEvent);
            } else {
                eventsList.add(streamEvent);
            }
        }
    }

    @Override
    public synchronized void delete(ComplexEventChunk deletingEventChunk, Operator operator) {
        if (indexAttribute != null) {
            operator.delete(deletingEventChunk, eventsMap);
        } else {
            operator.delete(deletingEventChunk, eventsList);
        }

    }

    @Override
    public synchronized void update(ComplexEventChunk updatingEventChunk, Operator operator,
                                    int[] mappingPosition) {
        if (indexAttribute != null) {
            operator.update(updatingEventChunk, eventsMap, mappingPosition);
        } else {
            operator.update(updatingEventChunk, eventsList, mappingPosition);
        }
    }

    @Override
    public void overwriteOrAdd(ComplexEventChunk overwritingOrAddingEventChunk, Operator operator,
                               int[] mappingPosition) {
        if (indexAttribute != null) {
            operator.overwriteOrAdd(overwritingOrAddingEventChunk, eventsMap, mappingPosition);
        } else {
            operator.overwriteOrAdd(overwritingOrAddingEventChunk, eventsList, mappingPosition);
        }
    }

    @Override
    public synchronized boolean contains(ComplexEvent matchingEvent, Finder finder) {
        if (indexAttribute != null) {
            return finder.contains(matchingEvent, eventsMap);
        } else {
            return finder.contains(matchingEvent, eventsList);
        }
    }

    @Override
    public synchronized StreamEvent find(ComplexEvent matchingEvent, Finder finder) {
        if (indexAttribute != null) {
            return finder.find(matchingEvent, eventsMap, streamEventCloner);
        } else {
            return finder.find(matchingEvent, eventsList, streamEventCloner);
        }
    }

    @Override
    public Finder constructFinder(Expression expression, MetaComplexEvent matchingMetaComplexEvent,
                                  ExecutionPlanContext executionPlanContext,
                                  List<VariableExpressionExecutor> variableExpressionExecutors,
                                  Map<String, EventTable> eventTableMap, int matchingStreamIndex, long withinTime) {
        return SyncOperatorParser.parse(expression, matchingMetaComplexEvent, executionPlanContext,
                variableExpressionExecutors, eventTableMap, matchingStreamIndex, tableDefinition, withinTime,
                indexAttribute, indexPosition, syncTableHandler);
    }

    @Override
    public Operator constructOperator(Expression expression, MetaComplexEvent metaComplexEvent,
                                      ExecutionPlanContext executionPlanContext,
                                      List<VariableExpressionExecutor> variableExpressionExecutors,
                                      Map<String, EventTable> eventTableMap, int matchingStreamIndex, long withinTime) {
        return SyncOperatorParser.parse(expression, metaComplexEvent, executionPlanContext,
                variableExpressionExecutors, eventTableMap, matchingStreamIndex, tableDefinition, withinTime,
                indexAttribute, indexPosition, syncTableHandler);
    }

    @Override
    public Object[] currentState() {
        return new Object[]{eventsList, eventsMap};
    }

    @Override
    public void restoreState(Object[] state) {
        eventsList = (LinkedList<StreamEvent>) state[0];
        eventsMap = (TreeMap<Object, StreamEvent>) state[1];
    }

    @Override
    public String getElementId() {
        return elementId;
    }

    public void setInMemoryEventMap(SortedMap<Object, StreamEvent> eventsMap) {
        this.eventsMap = eventsMap;
    }

    public void addToInMemoryEventMap(Object key, StreamEvent streamEvent) {
        if (indexAttribute != null) {
            this.eventsMap.put(key, streamEvent);
        } else {
            eventsList.add(streamEvent);
        }
    }

    public void removeFromMemoryEventMap(Object key) {
        this.eventsMap.remove(key);
    }

    public void subscribeForJmsEvents() {
        Properties properties = new Properties();
        try {
            ClassLoader classLoader = getClass().getClassLoader();
            properties.load(classLoader.getResourceAsStream("activemq.properties"));
            Hashtable<String, String> parameters = new Hashtable<String, String>();
            for (final String name : properties.stringPropertyNames()) {
                parameters.put(name, properties.getProperty(name));
            }

            String destination = "throttleData";
            JMSConnectionFactory jmsConnectionFactory = new JMSConnectionFactory(parameters, "Siddhi-JMS-Consumer");
            Map<String, String> messageConfig = new HashMap<String, String>();
            messageConfig.put(JMSConstants.PARAM_DESTINATION, destination);
            JMSTaskManager jmsTaskManager = JMSTaskManagerFactory.createTaskManagerForService(jmsConnectionFactory, "Siddhi-JMS-Consumer", new NativeWorkerPool(minThreadPoolSize, maxThreadPoolSize, KeepAliveTimeInMillis, jobQueueSize, "JMS Threads",
                    "JMSThreads" + UUID.randomUUID().toString()), messageConfig);
            jmsTaskManager.setJmsMessageListener(new JMSMessageListener(syncTableHandler, this));

            JMSListener jmsListener = new JMSListener("Siddhi-JMS-Consumer" + "#" + destination,
                    jmsTaskManager);
            jmsListener.startListener();
            log.info("Starting jms consumerQueue thread...");

        } catch (IOException e) {
            log.error("Cannot read properties file from resources. " + e.getMessage(), e);
        }
    }

}
