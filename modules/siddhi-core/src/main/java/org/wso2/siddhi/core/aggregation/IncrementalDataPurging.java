package org.wso2.siddhi.core.aggregation;

import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEventPool;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.table.Table;
import org.wso2.siddhi.core.util.SiddhiConstants;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.query.api.aggregation.TimePeriod;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.definition.AggregationDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 *
 * **/
public class IncrementalDataPurging implements Runnable {
    private Table table;
    private String interval;
    private String enable = "true";
    private List<Annotation> retention;
    private List<Annotation> annotations = new ArrayList<>();
    private long sec = Expression.Time.hour(1).value();
    private long mins = Expression.Time.hour(24).value();
    private long hours = Expression.Time.day(30).value();
    private long days = Expression.Time.year(5).value();
    private long months = 000;
    private long years = 000;
    private boolean isSystemTimeBased = false;
    private Map<TimePeriod.Duration, Long> retentionPeriods = new EnumMap<>(TimePeriod.Duration.class);
    private StreamEventPool streamEventPool;
    private Map<TimePeriod.Duration, Table> aggregationTables;

    public void init(AggregationDefinition aggregationDefinition, StreamEventPool streamEventPool,
                     Map<TimePeriod.Duration, Table> aggregationTables) {
        this.annotations = aggregationDefinition.getAnnotations();
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
        Expression timestampExpression = aggregationDefinition.getAggregateAttribute();

        if (timestampExpression == null) {
            isSystemTimeBased = true;
        }
        for (Annotation annotation : annotations) {
            annotationTypes.put(annotation.getName().toLowerCase(), annotation);
        }
        if (annotationTypes.keySet().contains(SiddhiConstants.NAMESPACE_PURGE)) {
            interval = annotationTypes.get(SiddhiConstants.NAMESPACE_PURGE).
                    getElement(SiddhiConstants.NAMESPACE_INTERVAL);
            if (Objects.nonNull(annotationTypes.get(SiddhiConstants.NAMESPACE_PURGE).
                    getElement(SiddhiConstants.ANNOTATION_ELEMENT_ENABLE))) {
                enable = annotationTypes.get(SiddhiConstants.NAMESPACE_PURGE).
                        getElement(SiddhiConstants.ANNOTATION_ELEMENT_ENABLE);
                if (!(enable.equals("true") || enable.equals("false"))) {
                    throw new SiddhiAppCreationException("Undefined value for enable: " + enable + "." +
                            " Please use true or false");
                }
            }
            retention = annotationTypes.get(SiddhiConstants.NAMESPACE_PURGE).
                    getAnnotations(SiddhiConstants.NAMESPACE_RETENTION);
            if (Objects.nonNull(retention)) {

            }
        }
    }

    @Override
    public void run() {
        long currentTime = System.currentTimeMillis();
        long purgeTime;
        Object[] purgeTimes = new Object[1];
        String compiledQuery;
        SortedMap<Integer, Object> parameters = new TreeMap<>();
        Attribute attribute;
        ComplexEventChunk<StateEvent> eventChunk = new ComplexEventChunk<>(true);
        CompiledCondition compiledCondition;
        BaseIncrimentalDataPurgingValueStore baseIncrimentalDataPurgingValueStore = new
                BaseIncrimentalDataPurgingValueStore(currentTime, streamEventPool);

        if (isSystemTimeBased) {
            compiledQuery = "AGG_TIMESTAMP > ?";
            attribute = new Attribute("AGG_TIMESTAMP", Attribute.Type.LONG);
            parameters.put(1, attribute);
        } else {
            compiledQuery = "";
            attribute = new Attribute("AGG_TIMESTAMP", Attribute.Type.LONG);
            parameters.put(1, attribute);
        }

        for (TimePeriod.Duration duration : aggregationTables.keySet()) {
            purgeTime = currentTime - retentionPeriods.get(duration);
            purgeTimes[0] = purgeTime;
            StateEvent secEvent = baseIncrimentalDataPurgingValueStore.createStreamEvent(purgeTimes);
            eventChunk.add(secEvent);
            compiledCondition = new IncrementalPurgeCompiledCondition(compiledQuery, parameters);
            table.deleteEvents(eventChunk, compiledCondition, 1);
        }

    }
}

