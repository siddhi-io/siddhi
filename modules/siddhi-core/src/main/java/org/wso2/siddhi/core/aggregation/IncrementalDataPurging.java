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

package org.wso2.siddhi.core.aggregation;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.MetaStateEvent;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventPool;
import org.wso2.siddhi.core.exception.DataPurgingException;
import org.wso2.siddhi.core.exception.NoSuchDurationException;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.table.Table;
import org.wso2.siddhi.core.util.SiddhiConstants;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import org.wso2.siddhi.query.api.aggregation.TimePeriod;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.annotation.Element;
import org.wso2.siddhi.query.api.definition.AggregationDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.TableDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.condition.Compare;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class implements the logic which is needed to purge data which are related to incremental
 **/
public class IncrementalDataPurging implements Runnable {
    private static final Logger LOG = Logger.getLogger(IncrementalDataPurging.class);
    private long purgeExecutionInterval = Expression.Time.minute(5).value();
    private long sec = Expression.Time.sec(30).value();
    private long mins = Expression.Time.hour(24).value();
    private long hours = Expression.Time.day(30).value();
    private long days = Expression.Time.year(5).value();
    private long months = -1;
    private long years = -1;
    private Map<TimePeriod.Duration, Long> retentionPeriods = new EnumMap<>(TimePeriod.Duration.class);
    private StreamEventPool streamEventPool;
    private Map<TimePeriod.Duration, Table> aggregationTables;
    private SiddhiAppContext siddhiAppContext;
    private Map<String, Table> tableMap;
    private static ScheduledFuture scheduledFuture;
    private static final String INTERNAL_AGG_TIMESTAMP_FIELD = "AGG_TIMESTAMP";


    public void init(AggregationDefinition aggregationDefinition, StreamEventPool streamEventPool,
                     Map<TimePeriod.Duration, Table> aggregationTables, SiddhiAppContext siddhiAppContext,
                     Map<String, Table> tableMap) {
        this.siddhiAppContext = siddhiAppContext;
        List<Annotation> annotations = aggregationDefinition.getAnnotations();
        this.tableMap = tableMap;
        this.streamEventPool = streamEventPool;
        this.aggregationTables = aggregationTables;
        for (TimePeriod.Duration duration : aggregationTables.keySet()) {
            switch (duration) {
                case SECONDS:
                    retentionPeriods.put(duration, sec);
                    break;
                case MINUTES:
                    retentionPeriods.put(duration, mins);
                    break;
                case HOURS:
                    retentionPeriods.put(duration, hours);
                    break;
                case DAYS:
                    retentionPeriods.put(duration, days);
                    break;
                case MONTHS:
                    retentionPeriods.put(duration, months);
                    break;
                case YEARS:
                    retentionPeriods.put(duration, years);
            }
        }
        Map<String, Annotation> annotationTypes = new HashMap<>();

        for (Annotation annotation : annotations) {
            annotationTypes.put(annotation.getName().toLowerCase(), annotation);
        }
        if (annotationTypes.keySet().contains(SiddhiConstants.NAMESPACE_PURGE)) {
            String interval = annotationTypes.get(SiddhiConstants.NAMESPACE_PURGE).
                    getElement(SiddhiConstants.NAMESPACE_INTERVAL);
            purgeExecutionInterval = timeToLong(interval);
            if (Objects.nonNull(annotationTypes.get(SiddhiConstants.NAMESPACE_PURGE).
                    getElement(SiddhiConstants.ANNOTATION_ELEMENT_ENABLE))) {
                String enable = annotationTypes.get(SiddhiConstants.NAMESPACE_PURGE).
                        getElement(SiddhiConstants.ANNOTATION_ELEMENT_ENABLE);
                if (!(enable.equals("true") || enable.equals("false"))) {
                    throw new SiddhiAppCreationException("Undefined value for enable: " + enable + "." +
                            " Please use true or false");
                }
            }
            List<Annotation> retentions = annotationTypes.get(SiddhiConstants.NAMESPACE_PURGE).
                    getAnnotations(SiddhiConstants.NAMESPACE_RETENTION);
            if (Objects.nonNull(retentions)) {
                Annotation retention = retentions.get(0);
                List<Element> elements = retention.getElements();
                for (Element element : elements) {
                    switch (durationNormalization(element.getKey())) {
                        case SECONDS:
                            if (element.getValue().equalsIgnoreCase("all")) {
                                sec = -1;
                            } else {
                                sec = timeToLong(element.getValue());
                            }
                            retentionPeriods.put(TimePeriod.Duration.SECONDS, sec);
                            break;
                        case MINUTES:
                            if (element.getValue().equalsIgnoreCase("all")) {
                                mins = -1;
                            } else {
                                mins = timeToLong(element.getValue());
                            }
                            retentionPeriods.put(TimePeriod.Duration.MINUTES, mins);
                            break;
                        case DAYS:
                            if (element.getValue().equalsIgnoreCase("all")) {
                                days = -1;
                            } else {
                                days = timeToLong(element.getValue());
                            }
                            retentionPeriods.put(TimePeriod.Duration.DAYS, days);
                            break;
                        case MONTHS:
                            if (element.getValue().equalsIgnoreCase("all")) {
                                months = -1;
                            } else {
                                months = timeToLong(element.getValue());
                            }
                            retentionPeriods.put(TimePeriod.Duration.MONTHS, months);
                            break;
                        case YEARS:
                            if (element.getValue().equalsIgnoreCase("all")) {
                                years = -1;
                            } else {
                                years = timeToLong(element.getValue());
                            }
                            retentionPeriods.put(TimePeriod.Duration.YEARS, years);
                            break;
                        case HOURS:
                            if (element.getValue().equalsIgnoreCase("all")) {
                                hours = -1;
                            } else {
                                hours = timeToLong(element.getValue());
                            }
                            retentionPeriods.put(TimePeriod.Duration.HOURS, hours);
                            break;
                    }
                }

            }
        }
    }

    public Long getPurgeExecutionInterval() {
        return purgeExecutionInterval;
    }

    @Override
    public void run() {
        long currentTime = System.currentTimeMillis();
        long purgeTime;
        Object[] purgeTimes = new Object[1];
        List<VariableExpressionExecutor> variableExpressionExecutorList = new ArrayList<>();
        Attribute attribute;
        VariableExpressionExecutor variableExpressionExecutor;
        ComplexEventChunk<StateEvent> eventChunk = new ComplexEventChunk<>(true);
        BaseIncrementalDataPurgingValueStore baseIncrementalDataPurgingValueStore = new
                BaseIncrementalDataPurgingValueStore(currentTime, streamEventPool);

        attribute = new Attribute(INTERNAL_AGG_TIMESTAMP_FIELD, Attribute.Type.LONG);
        variableExpressionExecutor = new VariableExpressionExecutor(attribute,
                0, 1);
        variableExpressionExecutorList.add(variableExpressionExecutor);

        for (Map.Entry<TimePeriod.Duration, Table> entry : aggregationTables.entrySet()) {
            if (retentionPeriods.get(entry.getKey()) != -1) {
                Variable leftVariable = new Variable(INTERNAL_AGG_TIMESTAMP_FIELD);
                leftVariable.setStreamId(entry.getValue().getTableDefinition().getId());
                Compare expression = new Compare(leftVariable,
                        Compare.Operator.LESS_THAN, new Variable(INTERNAL_AGG_TIMESTAMP_FIELD));
                purgeTime = currentTime - retentionPeriods.get(entry.getKey());
                purgeTimes[0] = purgeTime;
                StateEvent secEvent = baseIncrementalDataPurgingValueStore.createStreamEvent(purgeTimes);
                eventChunk.add(secEvent);
                Table table = aggregationTables.get(entry.getKey());
                try {
                    CompiledCondition compiledCondition = table.compileCondition(expression,
                            matchingMetaInfoHolder(table, attribute), siddhiAppContext, variableExpressionExecutorList,
                            tableMap, "query1");
                    LOG.info("Purging has enable for the table: " + table.getTableDefinition().getId() + " with a" +
                            " retention of timestamp : " + purgeTime);

                    table.deleteEvents(eventChunk, compiledCondition, 1);
                } catch (Exception e) {
                    LOG.error("Exception occurred while deleting events from " +
                            table.getTableDefinition().getId() + " table", e);
                    throw new DataPurgingException("Exception occurred while deleting events from " +
                            table.getTableDefinition().getId() + " table", e);
                }
            }
        }
    }

    /**
     * Convert user provided time in to long
     **/
    private Long timeToLong(String value) {
        Pattern timeValuePattern = Pattern.compile("\\d+");
        Pattern durationPattern = Pattern.compile("\\D+");
        Matcher timeMatcher = timeValuePattern.matcher(value);
        Matcher durationMatcher = durationPattern.matcher(value);
        int timeValue;
        TimePeriod.Duration duration;
        if (timeMatcher.find() && durationMatcher.find()) {
            duration = durationNormalization(durationMatcher.group(0).trim());
            timeValue = Integer.parseInt(timeMatcher.group(0));
            try {

                switch (duration) {
                    case SECONDS:
                        return Expression.Time.sec(timeValue).value();
                    case MINUTES:
                        return Expression.Time.minute(timeValue).value();
                    case HOURS:
                        return Expression.Time.hour(timeValue).value();
                    case DAYS:
                        return Expression.Time.day(timeValue).value();
                    case YEARS:
                        return Expression.Time.year(timeValue).value();
                    default:
                        return Expression.Time.month(timeValue).value();
                }
            } catch (NoSuchDurationException e) {
                throw new NoSuchDurationException("Duration" + timeValue + "does not exists", e);
            }
        } else {
            throw new NoSuchDurationException("Provided retention parameter cannot be identified. retention period:  "
                    + value + ".");
        }
    }

    /**
     * normalize user provided time durations
     */
    private TimePeriod.Duration durationNormalization(String value) {
        switch (value) {
            case "sec":
            case "seconds":
            case "second":
                return TimePeriod.Duration.SECONDS;
            case "min":
            case "minutes":
            case "minute":
                return TimePeriod.Duration.MINUTES;
            case "h":
            case "hour":
            case "hours":
                return TimePeriod.Duration.HOURS;
            case "days":
            case "day":
                return TimePeriod.Duration.DAYS;
            case "month":
            case "months":
                return TimePeriod.Duration.MONTHS;
            case "year":
            case "years":
                return TimePeriod.Duration.YEARS;
            default:
                throw new NoSuchDurationException("Duration '" + value + "' does not exists ");
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
        return new MatchingMetaInfoHolder(metaStateEvent,
                0, 1, deleteTableDefinition, definition, 0);
    }

    /**
     * Data purging task scheduler method
     **/
    public static void executeIncrementalDataPurging(SiddhiAppContext siddhiAppContext,
                                                     IncrementalDataPurging incrementalDataPurging) {

        if (Objects.nonNull(scheduledFuture)) {
            scheduledFuture.cancel(true);
            scheduledFuture = siddhiAppContext.getScheduledExecutorService().
                    scheduleWithFixedDelay(incrementalDataPurging, incrementalDataPurging.getPurgeExecutionInterval(),
                            incrementalDataPurging.getPurgeExecutionInterval(),
                            TimeUnit.MILLISECONDS);
        } else {
            scheduledFuture = siddhiAppContext.getScheduledExecutorService().
                    scheduleWithFixedDelay(incrementalDataPurging, incrementalDataPurging.getPurgeExecutionInterval(),
                            incrementalDataPurging.getPurgeExecutionInterval(),
                            TimeUnit.MILLISECONDS);
        }
    }
}

