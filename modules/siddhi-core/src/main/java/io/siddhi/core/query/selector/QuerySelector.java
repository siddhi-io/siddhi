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
package io.siddhi.core.query.selector;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.GroupedComplexEvent;
import io.siddhi.core.event.state.populater.StateEventPopulator;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.executor.condition.ConditionExpressionExecutor;
import io.siddhi.core.query.output.ratelimit.OutputRateLimiter;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.selector.attribute.processor.AttributeProcessor;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.query.api.execution.query.selection.Selector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Processor implementation representing selector portion of the Siddhi query.
 */
public class QuerySelector implements Processor {


    private static final Logger log = LogManager.getLogger(QuerySelector.class);
    private Selector selector;
    private SiddhiQueryContext siddhiQueryContext;
    private boolean currentOn = false;
    private boolean expiredOn = false;
    private boolean containsAggregator = false;
    private OutputRateLimiter outputRateLimiter;
    private List<AttributeProcessor> attributeProcessorList;
    private ConditionExpressionExecutor havingConditionExecutor = null;
    private boolean isGroupBy = false;
    private GroupByKeyGenerator groupByKeyGenerator;
    private boolean isOrderBy = false;
    private OrderByEventComparator orderByEventComparator;
    private String id;
    private StateEventPopulator eventPopulator;
    private boolean batchingEnabled = true;
    private long limit = SiddhiConstants.UNKNOWN_STATE;
    private long offset = SiddhiConstants.UNKNOWN_STATE;

    public QuerySelector(String id, Selector selector, boolean currentOn, boolean expiredOn, SiddhiQueryContext
            siddhiQueryContext) {
        this.id = id;
        this.currentOn = currentOn;
        this.expiredOn = expiredOn;
        this.selector = selector;
        this.siddhiQueryContext = siddhiQueryContext;
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        if (log.isTraceEnabled()) {
            log.trace("event is processed by selector " + id + this);
        }
        ComplexEventChunk outputComplexEventChunk = null;
        if (complexEventChunk.isBatch() && batchingEnabled) {
            if (isGroupBy) {
                outputComplexEventChunk = processInBatchGroupBy(complexEventChunk);
            } else if (containsAggregator) {
                outputComplexEventChunk = processInBatchNoGroupBy(complexEventChunk);
            } else {
                outputComplexEventChunk = processNoGroupBy(complexEventChunk);
            }
        } else {
            if (isGroupBy) {
                outputComplexEventChunk = processGroupBy(complexEventChunk);
            } else {
                outputComplexEventChunk = processNoGroupBy(complexEventChunk);
            }
        }
        if (outputComplexEventChunk != null) {
            outputRateLimiter.process(outputComplexEventChunk);
        }
    }

    public void process(List<ComplexEventChunk> complexEventChunks) {
        List<ComplexEventChunk> returnEventChunks = new ArrayList<>(complexEventChunks.size());
        for (ComplexEventChunk complexEventChunk : complexEventChunks) {
            if (complexEventChunk.getFirst() != null) {
                ComplexEventChunk returnEventChunk;
                if (complexEventChunk instanceof SelectorTypeComplexEventChunk &&
                        ((SelectorTypeComplexEventChunk) complexEventChunk).isProcessPassThrough()) {
                    returnEventChunk = executePassThrough(complexEventChunk);
                } else {
                    returnEventChunk = execute(complexEventChunk);
                }
                if (returnEventChunk != null) {
                    returnEventChunks.add(returnEventChunk);
                }
            }
        }
        if (!returnEventChunks.isEmpty()) {
            outputRateLimiter.process(returnEventChunks);
        }
    }

    private ComplexEventChunk executePassThrough(ComplexEventChunk complexEventChunk) {
        complexEventChunk.reset();
        synchronized (this) {
            while (complexEventChunk.hasNext()) {
                ComplexEvent event = complexEventChunk.next();
                if (((event.getType() != StreamEvent.Type.CURRENT || !currentOn) &&
                        (event.getType() != StreamEvent.Type.EXPIRED || !expiredOn))) {
                    complexEventChunk.remove();
                }
            }
        }
        complexEventChunk.reset();
        if (complexEventChunk.hasNext()) {
            return complexEventChunk;
        }
        return null;
    }

    public ComplexEventChunk execute(ComplexEventChunk complexEventChunk) {
        if (log.isTraceEnabled()) {
            log.trace("event is executed by selector " + id + this);
        }
        if (batchingEnabled) {
            if (isGroupBy) {
                return processInBatchGroupBy(complexEventChunk);
            } else if (containsAggregator) {
                return processInBatchNoGroupBy(complexEventChunk);
            } else {
                return processNoGroupBy(complexEventChunk);
            }
        } else {
            if (isGroupBy) {
                return processGroupBy(complexEventChunk);
            } else {
                return processNoGroupBy(complexEventChunk);
            }
        }
    }

    private ComplexEventChunk processNoGroupBy(ComplexEventChunk complexEventChunk) {
        complexEventChunk.reset();
        synchronized (this) {
            while (complexEventChunk.hasNext()) {
                ComplexEvent event = complexEventChunk.next();
                switch (event.getType()) {

                    case CURRENT:
                    case EXPIRED:
                        eventPopulator.populateStateEvent(event);
                        for (AttributeProcessor attributeProcessor : attributeProcessorList) {
                            attributeProcessor.process(event);
                        }
                        if (((event.getType() != StreamEvent.Type.CURRENT || !currentOn) && (event.getType() !=
                                StreamEvent.Type.EXPIRED || !expiredOn)) || ((havingConditionExecutor != null &&
                                !havingConditionExecutor.execute(event)))) {
                            complexEventChunk.remove();
                        }
                        break;
                    case RESET:
                        for (AttributeProcessor attributeProcessor : attributeProcessorList) {
                            attributeProcessor.process(event);
                        }
                        break;
                    case TIMER:
                        complexEventChunk.remove();
                        break;
                }
            }
        }
        if (isOrderBy) {
            orderEventChunk(complexEventChunk);
        }
        if (offset != SiddhiConstants.UNKNOWN_STATE) {
            offsetEventChunk(complexEventChunk);
        }
        if (limit != SiddhiConstants.UNKNOWN_STATE) {
            limitEventChunk(complexEventChunk);
        }
        complexEventChunk.reset();
        if (complexEventChunk.hasNext()) {
            return complexEventChunk;
        }
        return null;
    }

    private ComplexEventChunk<ComplexEvent> processGroupBy(ComplexEventChunk complexEventChunk) {
        complexEventChunk.reset();
        ComplexEventChunk<ComplexEvent> currentComplexEventChunk = new ComplexEventChunk<ComplexEvent>
                ();

        synchronized (this) {
            int limitCount = 0;
            while (complexEventChunk.hasNext()) {
                ComplexEvent event = complexEventChunk.next();
                switch (event.getType()) {
                    case CURRENT:
                    case EXPIRED:
                        eventPopulator.populateStateEvent(event);
                        String groupByKey = groupByKeyGenerator.constructEventKey(event);
                        SiddhiAppContext.startGroupByFlow(groupByKey);
                        try {
                            for (AttributeProcessor attributeProcessor : attributeProcessorList) {
                                attributeProcessor.process(event);
                            }
                            if ((event.getType() == StreamEvent.Type.CURRENT && currentOn) || (event.getType() ==
                                    StreamEvent.Type.EXPIRED && expiredOn)) {
                                if (!(havingConditionExecutor != null && !havingConditionExecutor.execute(event))) {
                                    complexEventChunk.remove();
                                    if (limit == SiddhiConstants.UNKNOWN_STATE) {
                                        currentComplexEventChunk.add(new GroupedComplexEvent(groupByKey, event));
                                    } else {
                                        if (limitCount < limit) {
                                            currentComplexEventChunk.add(new GroupedComplexEvent(groupByKey, event));
                                            limitCount++;
                                        }
                                    }
                                }
                            }
                        } finally {
                            SiddhiAppContext.stopGroupByFlow();
                        }
                        break;
                    case TIMER:
                        break;
                    case RESET:
                        for (AttributeProcessor attributeProcessor : attributeProcessorList) {
                            attributeProcessor.process(event);
                        }
                        break;
                }

            }
        }
        if (isOrderBy) {
            orderEventChunk(complexEventChunk);
        }
        if (offset != SiddhiConstants.UNKNOWN_STATE) {
            offsetEventChunk(complexEventChunk);
        }
        if (limit != SiddhiConstants.UNKNOWN_STATE) {
            limitEventChunk(complexEventChunk);
        }
        currentComplexEventChunk.reset();
        if (currentComplexEventChunk.hasNext()) {
            return currentComplexEventChunk;
        }
        return null;
    }

    private ComplexEventChunk processInBatchNoGroupBy(ComplexEventChunk complexEventChunk) {
        complexEventChunk.reset();
        ComplexEvent lastEvent = null;

        synchronized (this) {
            while (complexEventChunk.hasNext()) {
                ComplexEvent event = complexEventChunk.next();
                switch (event.getType()) {
                    case CURRENT:
                    case EXPIRED:
                        eventPopulator.populateStateEvent(event);
                        for (AttributeProcessor attributeProcessor : attributeProcessorList) {
                            attributeProcessor.process(event);
                        }
                        if (!(havingConditionExecutor != null && !havingConditionExecutor.execute(event))) {
                            if ((event.getType() == StreamEvent.Type.CURRENT && currentOn) || (event.getType() ==
                                    StreamEvent.Type.EXPIRED && expiredOn)) {
                                complexEventChunk.remove();
                                lastEvent = event;
                            }
                        }
                        break;
                    case TIMER:
                        break;
                    case RESET:
                        for (AttributeProcessor attributeProcessor : attributeProcessorList) {
                            attributeProcessor.process(event);
                        }
                        break;
                }
            }
        }

        if (lastEvent != null) {
            complexEventChunk.clear();
            if ((offset == SiddhiConstants.UNKNOWN_STATE || offset == 0) &&
                    (limit == SiddhiConstants.UNKNOWN_STATE || limit > 0)) {
                complexEventChunk.add(lastEvent);
            }
            return complexEventChunk;
        }
        return null;
    }

    private ComplexEventChunk processInBatchGroupBy(ComplexEventChunk complexEventChunk) {
        Map<String, ComplexEvent> groupedEvents = new LinkedHashMap<String, ComplexEvent>();
        complexEventChunk.reset();

        synchronized (this) {
            while (complexEventChunk.hasNext()) {
                ComplexEvent event = complexEventChunk.next();
                switch (event.getType()) {

                    case CURRENT:
                    case EXPIRED:
                        eventPopulator.populateStateEvent(event);
                        String groupByKey = groupByKeyGenerator.constructEventKey(event);
                        SiddhiAppContext.startGroupByFlow(groupByKey);
                        try {
                            for (AttributeProcessor attributeProcessor : attributeProcessorList) {
                                attributeProcessor.process(event);
                            }
                            if (!(havingConditionExecutor != null && !havingConditionExecutor.execute(event))) {
                                if ((event.getType() == StreamEvent.Type.CURRENT && currentOn) || (event.getType() ==
                                        StreamEvent.Type.EXPIRED && expiredOn)) {
                                    complexEventChunk.remove();
                                    groupedEvents.put(groupByKey, event);
                                }
                            }
                        } finally {
                            SiddhiAppContext.stopGroupByFlow();
                        }
                        break;
                    case TIMER:
                        break;
                    case RESET:
                        for (AttributeProcessor attributeProcessor : attributeProcessorList) {
                            attributeProcessor.process(event);
                        }
                        break;
                }
            }
        }

        if (groupedEvents.size() != 0) {
            complexEventChunk.clear();
            for (Map.Entry<String, ComplexEvent> groupedEventEntry : groupedEvents.entrySet()) {
                complexEventChunk.add(new GroupedComplexEvent(groupedEventEntry.getKey(),
                        groupedEventEntry.getValue()));
            }
            if (isOrderBy) {
                orderEventChunk(complexEventChunk);
            }
            if (offset != SiddhiConstants.UNKNOWN_STATE) {
                offsetEventChunk(complexEventChunk);
            }
            if (limit != SiddhiConstants.UNKNOWN_STATE) {
                limitEventChunk(complexEventChunk);
            }
            complexEventChunk.reset();
            return complexEventChunk;
        }
        return null;
    }

    @Override
    public Processor getNextProcessor() {
        return null;    //since there is no processors after a query selector
    }

    public void setNextProcessor(OutputRateLimiter outputRateLimiter) {
        if (this.outputRateLimiter == null) {
            this.outputRateLimiter = outputRateLimiter;
        } else {
            throw new SiddhiAppCreationException("outputRateLimiter is already assigned");
        }
    }

    @Override
    public void setNextProcessor(Processor processor) {
        //this method will not be used as there is no processors after a query selector
    }

    @Override
    public void setToLast(Processor processor) {
        if (getNextProcessor() == null) {
            this.setNextProcessor(processor);
        } else {
            getNextProcessor().setToLast(processor);
        }
    }

    public List<AttributeProcessor> getAttributeProcessorList() {
        return attributeProcessorList;
    }

    public void setAttributeProcessorList(List<AttributeProcessor> attributeProcessorList, boolean containsAggregator) {
        this.attributeProcessorList = attributeProcessorList;
        this.containsAggregator = this.containsAggregator || containsAggregator;
    }

    public void setGroupByKeyGenerator(GroupByKeyGenerator groupByKeyGenerator) {
        isGroupBy = true;
        this.groupByKeyGenerator = groupByKeyGenerator;
    }

    public void setOrderByEventComparator(OrderByEventComparator orderByEventComparator) {
        isOrderBy = true;
        this.orderByEventComparator = orderByEventComparator;
    }

    public void setHavingConditionExecutor(ConditionExpressionExecutor havingConditionExecutor, boolean
            containsAggregator) {
        this.havingConditionExecutor = havingConditionExecutor;
        this.containsAggregator = this.containsAggregator || containsAggregator;
    }

    public void setBatchingEnabled(boolean batchingEnabled) {
        this.batchingEnabled = batchingEnabled;
    }

    public void setEventPopulator(StateEventPopulator eventPopulator) {
        this.eventPopulator = eventPopulator;
    }

    public void setLimit(long limit) {
        if (limit < 0) {
            throw new SiddhiAppCreationException("'limit' cannot have negative value, but found '" + limit + "'",
                    selector, siddhiQueryContext.getSiddhiAppContext());
        }
        this.limit = limit;
    }

    public void setOffset(long offset) {
        if (offset < 0) {
            throw new SiddhiAppCreationException("'offset' cannot have negative value, but found '" + offset + "'",
                    selector, siddhiQueryContext.getSiddhiAppContext());
        }
        this.offset = offset;
    }

    private void orderEventChunk(ComplexEventChunk complexEventChunk) {
        ComplexEventChunk orderingComplexEventChunk = new ComplexEventChunk();
        List<ComplexEvent> eventList = new ArrayList<>();

        ComplexEvent.Type currentEventType = null;
        complexEventChunk.reset();
        if (complexEventChunk.getFirst() != null) {
            currentEventType = complexEventChunk.getFirst().getType();
            while (complexEventChunk.hasNext()) {
                ComplexEvent event = complexEventChunk.next();
                complexEventChunk.remove();
                if (currentEventType == event.getType()) {
                    eventList.add(event);
                } else {
                    currentEventType = event.getType();
                    eventList.sort(orderByEventComparator);
                    for (ComplexEvent complexEvent : eventList) {
                        orderingComplexEventChunk.add(complexEvent);
                    }
                    eventList.clear();
                    eventList.add(event);
                }
            }
            eventList.sort(orderByEventComparator);
            for (ComplexEvent complexEvent : eventList) {
                orderingComplexEventChunk.add(complexEvent);
            }
            complexEventChunk.clear();
            complexEventChunk.add(orderingComplexEventChunk.getFirst());
        }

    }

    private void limitEventChunk(ComplexEventChunk complexEventChunk) {
        complexEventChunk.reset();
        int limitCount = 0;
        while (complexEventChunk.hasNext()) {
            ComplexEvent event = complexEventChunk.next();
            if (event.getType() == StreamEvent.Type.CURRENT || event.getType() == StreamEvent.Type.EXPIRED) {
                if ((limit > limitCount) &&
                        (event.getType() == StreamEvent.Type.CURRENT && currentOn) ||
                        (event.getType() == StreamEvent.Type.EXPIRED && expiredOn)) {
                    limitCount++;
                } else {
                    complexEventChunk.remove();
                }
            }
        }
    }

    private void offsetEventChunk(ComplexEventChunk complexEventChunk) {
        complexEventChunk.reset();
        int offsetCount = 0;
        while (complexEventChunk.hasNext()) {
            ComplexEvent event = complexEventChunk.next();
            if (event.getType() == StreamEvent.Type.CURRENT || event.getType() == StreamEvent.Type.EXPIRED) {
                if (offset > offsetCount) {
                    if ((event.getType() == StreamEvent.Type.CURRENT && currentOn) ||
                            (event.getType() == StreamEvent.Type.EXPIRED && expiredOn)) {
                        offsetCount++;
                    }
                    complexEventChunk.remove();
                } else {
                    break;
                }
            }
        }
    }
}
