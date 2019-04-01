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

package io.siddhi.core.aggregation;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.MetaStateEvent;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.exception.DataPurgingException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.query.api.aggregation.TimePeriod;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.annotation.Element;
import io.siddhi.query.api.definition.AggregationDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.Variable;
import io.siddhi.query.api.expression.condition.Compare;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static io.siddhi.query.api.expression.Expression.Time.normalizeDuration;
import static io.siddhi.query.api.expression.Expression.Time.timeToLong;

/**
 * This class implements the logic which is needed to purge data which are related to incremental
 **/
public class IncrementalDataPurging implements Runnable {
    private static final Logger LOG = Logger.getLogger(IncrementalDataPurging.class);
    private static final String INTERNAL_AGG_TIMESTAMP_FIELD = "AGG_TIMESTAMP";
    private static final String EXTERNAL_AGG_TIMESTAMP_FIELD = "AGG_EVENT_TIMESTAMP";
    private static final Long RETAIN_ALL = -1L;
    private static final String RETAIN_ALL_VALUES = "all";
    private long purgeExecutionInterval = Expression.Time.minute(15).value();
    private boolean purgingEnabled = true;
    private Map<TimePeriod.Duration, Long> retentionPeriods = new EnumMap<>(TimePeriod.Duration.class);
    private StreamEventFactory streamEventFactory;
    private Map<TimePeriod.Duration, Table> aggregationTables;
    private SiddhiQueryContext siddhiQueryContext;
    private ScheduledFuture scheduledPurgingTaskStatus;
    private String purgingTimestampField;
    private Map<TimePeriod.Duration, Long> minimumDurationMap = new EnumMap<>(TimePeriod.Duration.class);
    private ComplexEventChunk<StateEvent> eventChunk = new ComplexEventChunk<>(true);
    private List<VariableExpressionExecutor> variableExpressionExecutorList = new ArrayList<>();
    private Attribute aggregatedTimestampAttribute;
    private Map<TimePeriod.Duration, CompiledCondition> compiledConditionsHolder =
            new EnumMap<>(TimePeriod.Duration.class);
    private Map<String, Table> tableMap = new HashMap<>();
    private AggregationDefinition aggregationDefinition;

    public void init(AggregationDefinition aggregationDefinition, StreamEventFactory streamEventFactory,
                     Map<TimePeriod.Duration, Table> aggregationTables, Boolean isProcessingOnExternalTime,
                     SiddhiQueryContext siddhiQueryContext) {
        this.siddhiQueryContext = siddhiQueryContext;
        this.aggregationDefinition = aggregationDefinition;
        List<Annotation> annotations = aggregationDefinition.getAnnotations();
        this.streamEventFactory = streamEventFactory;
        this.aggregationTables = aggregationTables;
        if (isProcessingOnExternalTime) {
            purgingTimestampField = EXTERNAL_AGG_TIMESTAMP_FIELD;
        } else {
            purgingTimestampField = INTERNAL_AGG_TIMESTAMP_FIELD;
        }
        aggregatedTimestampAttribute = new Attribute(purgingTimestampField, Attribute.Type.LONG);

        VariableExpressionExecutor variableExpressionExecutor = new VariableExpressionExecutor(
                aggregatedTimestampAttribute, 0, 1);
        variableExpressionExecutorList.add(variableExpressionExecutor);
        for (Map.Entry<TimePeriod.Duration, Table> entry : aggregationTables.entrySet()) {
            this.tableMap.put(entry.getValue().getTableDefinition().getId(), entry.getValue());
            switch (entry.getKey()) {
                case SECONDS:
                    retentionPeriods.put(entry.getKey(), Expression.Time.sec(120).value());
                    minimumDurationMap.put(entry.getKey(), Expression.Time.sec(120).value());
                    break;
                case MINUTES:
                    retentionPeriods.put(entry.getKey(), Expression.Time.hour(24).value());
                    minimumDurationMap.put(entry.getKey(), Expression.Time.minute(120).value());
                    break;
                case HOURS:
                    retentionPeriods.put(entry.getKey(), Expression.Time.day(30).value());
                    minimumDurationMap.put(entry.getKey(), Expression.Time.hour(25).value());
                    break;
                case DAYS:
                    retentionPeriods.put(entry.getKey(), Expression.Time.year(1).value());
                    minimumDurationMap.put(entry.getKey(), Expression.Time.day(32).value());
                    break;
                case MONTHS:
                    retentionPeriods.put(entry.getKey(), RETAIN_ALL);
                    minimumDurationMap.put(entry.getKey(), Expression.Time.month(13).value());
                    break;
                case YEARS:
                    retentionPeriods.put(entry.getKey(), RETAIN_ALL);
                    minimumDurationMap.put(entry.getKey(), 0L);
            }
        }

        Map<String, Annotation> annotationTypes = new HashMap<>();
        for (Annotation annotation : annotations) {
            annotationTypes.put(annotation.getName().toLowerCase(), annotation);
        }
        Annotation purge = annotationTypes.get(SiddhiConstants.NAMESPACE_PURGE);
        if (purge != null) {
            if (purge.getElement(SiddhiConstants.ANNOTATION_ELEMENT_ENABLE) != null) {
                String purgeEnable = purge.getElement(SiddhiConstants.ANNOTATION_ELEMENT_ENABLE);
                if (!("true".equalsIgnoreCase(purgeEnable) || "false".equalsIgnoreCase(purgeEnable))) {
                    throw new SiddhiAppCreationException("Invalid value for enable: " + purgeEnable + "." +
                            " Please use true or false");
                } else {
                    purgingEnabled = Boolean.parseBoolean(purgeEnable);
                }
            }
            if (purgingEnabled) {
                // If interval is defined, default value of 15 min will be replaced by user input value
                if (purge.getElement(SiddhiConstants.ANNOTATION_ELEMENT_INTERVAL) != null) {
                    String interval = purge.getElement(SiddhiConstants.ANNOTATION_ELEMENT_INTERVAL);
                    purgeExecutionInterval = timeToLong(interval);
                }
                List<Annotation> retentions = purge.getAnnotations(SiddhiConstants.NAMESPACE_RETENTION_PERIOD);
                if (retentions != null && !retentions.isEmpty()) {
                    Annotation retention = retentions.get(0);
                    List<Element> elements = retention.getElements();
                    for (Element element : elements) {
                        TimePeriod.Duration duration = normalizeDuration(element.getKey());
                        if (!aggregationTables.keySet().contains(duration)) {
                            throw new SiddhiAppCreationException(duration + " granularity cannot be purged since " +
                                    "aggregation has not performed in " + duration + " granularity");
                        }
                        if (element.getValue().equalsIgnoreCase(RETAIN_ALL_VALUES)) {
                            retentionPeriods.put(duration, RETAIN_ALL);
                        } else {
                            if (timeToLong(element.getValue()) >= minimumDurationMap.get(duration)) {
                                retentionPeriods.put(duration, timeToLong(element.getValue()));
                            } else {
                                throw new SiddhiAppCreationException(duration + " granularity cannot be purge" +
                                        " with a retention of '" + element.getValue() + "', minimum retention" +
                                        " should be greater  than " + TimeUnit.MILLISECONDS.toMinutes
                                        (minimumDurationMap.get(duration)) + " minutes");
                            }
                        }
                    }
                }
            }
        }
        compiledConditionsHolder = createCompileConditions(aggregationTables, tableMap);
    }

    public boolean isPurgingEnabled() {
        return purgingEnabled;
    }

    public void setPurgingEnabled(boolean purgingEnabled) {
        this.purgingEnabled = purgingEnabled;
    }

    @Override
    public void run() {
        long currentTime = System.currentTimeMillis();
        long purgeTime;
        Object[] purgeTimeArray = new Object[1];
        for (Map.Entry<TimePeriod.Duration, Table> entry : aggregationTables.entrySet()) {
            if (!retentionPeriods.get(entry.getKey()).equals(RETAIN_ALL)) {
                eventChunk.clear();
                purgeTime = currentTime - retentionPeriods.get(entry.getKey());
                purgeTimeArray[0] = purgeTime;
                StateEvent secEvent = createStreamEvent(purgeTimeArray, currentTime);
                eventChunk.add(secEvent);
                Table table = aggregationTables.get(entry.getKey());
                try {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Purging data of table: " + table.getTableDefinition().getId() + " with a" +
                                " retention of timestamp : " + purgeTime);
                    }
                    table.deleteEvents(eventChunk, compiledConditionsHolder.get(entry.getKey()), 1);
                } catch (RuntimeException e) {
                    LOG.error("Exception occurred while deleting events from " +
                            table.getTableDefinition().getId() + " table", e);
                    throw new DataPurgingException("Exception occurred while deleting events from " +
                            table.getTableDefinition().getId() + " table", e);
                }
            }
        }
    }

    /**
     * Building the MatchingMetaInfoHolder for delete records
     **/
    private MatchingMetaInfoHolder matchingMetaInfoHolder(Table table, Attribute attribute) {
        MetaStateEvent metaStateEvent = new MetaStateEvent(2);
        MetaStreamEvent metaStreamEventWithDeletePara = new MetaStreamEvent();
        MetaStreamEvent metaStreamEventForTable = new MetaStreamEvent();
        TableDefinition deleteTableDefinition = TableDefinition.id("");
        deleteTableDefinition.attribute(attribute.getName(), attribute.getType());
        metaStreamEventWithDeletePara.setEventType(MetaStreamEvent.EventType.TABLE);
        metaStreamEventWithDeletePara.addOutputData(attribute);
        metaStreamEventWithDeletePara.addInputDefinition(deleteTableDefinition);
        metaStreamEventForTable.setEventType(MetaStreamEvent.EventType.TABLE);
        for (Attribute attributes : table.getTableDefinition().getAttributeList()) {
            metaStreamEventForTable.addOutputData(attributes);
        }
        metaStreamEventForTable.addInputDefinition(table.getTableDefinition());
        metaStateEvent.addEvent(metaStreamEventWithDeletePara);
        metaStateEvent.addEvent(metaStreamEventForTable);
        TableDefinition definition = table.getTableDefinition();
        return new MatchingMetaInfoHolder(metaStateEvent, 0, 1, deleteTableDefinition, definition, 0);
    }

    /**
     * Building the compiled conditions for purge data
     **/
    private Map<TimePeriod.Duration, CompiledCondition> createCompileConditions(
            Map<TimePeriod.Duration, Table> aggregationTables, Map<String, Table> tableMap) {
        Map<TimePeriod.Duration, CompiledCondition> compiledConditionMap = new EnumMap<>(TimePeriod.Duration.class);
        CompiledCondition compiledCondition;
        Table table;
        for (Map.Entry<TimePeriod.Duration, Table> entry : aggregationTables.entrySet()) {
            if (!retentionPeriods.get(entry.getKey()).equals(RETAIN_ALL)) {
                table = aggregationTables.get(entry.getKey());
                Variable leftVariable = new Variable(purgingTimestampField);
                leftVariable.setStreamId(entry.getValue().getTableDefinition().getId());
                Compare expression = new Compare(leftVariable,
                        Compare.Operator.LESS_THAN, new Variable(purgingTimestampField));
                compiledCondition = table.compileCondition(expression,
                        matchingMetaInfoHolder(table, aggregatedTimestampAttribute), variableExpressionExecutorList,
                        tableMap, siddhiQueryContext);
                compiledConditionMap.put(entry.getKey(), compiledCondition);
            }
        }
        return compiledConditionMap;
    }

    /**
     * Data purging task scheduler method
     **/
    public void executeIncrementalDataPurging() {
        StringBuilder tableNames = new StringBuilder();
        if (isPurgingEnabled()) {
            if (scheduledPurgingTaskStatus != null) {
                scheduledPurgingTaskStatus.cancel(true);
                scheduledPurgingTaskStatus = siddhiQueryContext.getSiddhiAppContext().getScheduledExecutorService().
                        scheduleWithFixedDelay(this, purgeExecutionInterval, purgeExecutionInterval,
                                TimeUnit.MILLISECONDS);
            } else {
                scheduledPurgingTaskStatus = siddhiQueryContext.getSiddhiAppContext().getScheduledExecutorService().
                        scheduleWithFixedDelay(this, purgeExecutionInterval, purgeExecutionInterval,
                                TimeUnit.MILLISECONDS);
            }
            for (Map.Entry<TimePeriod.Duration, Long> entry : retentionPeriods.entrySet()) {
                if (!retentionPeriods.get(entry.getKey()).equals(RETAIN_ALL)) {
                    tableNames.append(entry.getKey()).append(",");
                }
            }
            LOG.info("Data purging has enabled for tables: " + tableNames + " with an interval of " +
                    ((purgeExecutionInterval) / 1000) + " seconds in " + aggregationDefinition.getId() +
                    " aggregation");
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Purging is disabled in siddhi app: " + siddhiQueryContext.getSiddhiAppContext().getName());
            }
        }
    }

    /**
     * creating stream event method
     **/
    private StateEvent createStreamEvent(Object[] values, Long timestamp) {
        StreamEvent streamEvent = streamEventFactory.newInstance();
        streamEvent.setTimestamp(timestamp);
        streamEvent.setOutputData(values);
        StateEvent stateEvent = new StateEvent(2, 1);
        stateEvent.addEvent(0, streamEvent);
        return stateEvent;
    }
}

