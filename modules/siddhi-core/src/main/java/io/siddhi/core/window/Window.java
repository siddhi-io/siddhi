/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.siddhi.core.window;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.event.stream.converter.ZeroStreamEventConverter;
import io.siddhi.core.exception.OperationNotSupportedException;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.input.stream.single.EntryValveProcessor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.SchedulingProcessor;
import io.siddhi.core.query.processor.stream.window.FindableProcessor;
import io.siddhi.core.query.processor.stream.window.WindowProcessor;
import io.siddhi.core.stream.StreamJunction;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.Scheduler;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.lock.LockWrapper;
import io.siddhi.core.util.parser.SchedulerParser;
import io.siddhi.core.util.parser.SingleInputStreamParser;
import io.siddhi.core.util.parser.helper.QueryParserHelper;
import io.siddhi.core.util.statistics.LatencyTracker;
import io.siddhi.core.util.statistics.MemoryCalculable;
import io.siddhi.core.util.statistics.ThroughputTracker;
import io.siddhi.core.util.statistics.metrics.Level;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.WindowDefinition;
import io.siddhi.query.api.execution.query.output.stream.OutputStream;
import io.siddhi.query.api.expression.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Window implementation of SiddhiQL.
 * It can be seen as a global Window which can be accessed from multiple queries.
 */
public class Window implements FindableProcessor, MemoryCalculable {

    /**
     * WindowDefinition used to construct this window.
     */
    private final WindowDefinition windowDefinition;

    /**
     * SiddhiAppContext is used to create the elementId  and WindowProcessor.
     */
    private final SiddhiAppContext siddhiAppContext;
    /**
     * LockWrapper to coordinate asynchronous events.
     */
    private final LockWrapper lockWrapper;
    /**
     * TemplateBuilder to convert {@link StateEvent}s to {@link StreamEvent}s
     */
    private final ZeroStreamEventConverter eventConverter = new ZeroStreamEventConverter();
    /**
     * Publisher to which the output events from internal window have to be sent.
     */
    private StreamJunction.Publisher outputPublisher;
    /**
     * Processor for the internal window.
     * It will contain the PublisherProcessor as the last windowProcessor in the chain.
     */
    private Processor windowProcessor;
    /**
     * WindowProcessor reference to the actual processor which is holding the events.
     * If this.windowProcessor refers to EntryValveProcessor (if the Window is a scheduler based, it may be),
     * internalWindowProcessor refers to the this.windowProcessor.getNextProcessor()
     */
    private WindowProcessor internalWindowProcessor;
    /**
     * StreamEventFactory to create new empty StreamEvent.
     */
    private StreamEventFactory streamEventFactory;

    /**
     * window operation latency and throughput trackers
     */
    private LatencyTracker latencyTrackerInsert;
    private LatencyTracker latencyTrackerFind;
    private ThroughputTracker throughputTrackerFind;
    private ThroughputTracker throughputTrackerInsert;


    /**
     * Construct a Window object.
     *
     * @param windowDefinition definition of the window
     * @param siddhiAppContext siddhi app context of Siddhi
     */
    public Window(WindowDefinition windowDefinition, SiddhiAppContext siddhiAppContext) {
        this.windowDefinition = windowDefinition;
        this.siddhiAppContext = siddhiAppContext;
        this.lockWrapper = new LockWrapper(windowDefinition.getId());
        this.lockWrapper.setLock(new ReentrantLock());
        if (siddhiAppContext.getStatisticsManager() != null) {
            latencyTrackerFind = QueryParserHelper.createLatencyTracker(siddhiAppContext, windowDefinition.getId(),
                    SiddhiConstants.METRIC_INFIX_WINDOWS, SiddhiConstants.METRIC_TYPE_FIND);
            latencyTrackerInsert = QueryParserHelper.createLatencyTracker(siddhiAppContext, windowDefinition.getId(),
                    SiddhiConstants.METRIC_INFIX_WINDOWS, SiddhiConstants.METRIC_TYPE_INSERT);

            throughputTrackerFind = QueryParserHelper.createThroughputTracker(siddhiAppContext,
                    windowDefinition.getId(), SiddhiConstants.METRIC_INFIX_WINDOWS, SiddhiConstants.METRIC_TYPE_FIND);
            throughputTrackerInsert = QueryParserHelper.createThroughputTracker(siddhiAppContext,
                    windowDefinition.getId(), SiddhiConstants.METRIC_INFIX_WINDOWS, SiddhiConstants.METRIC_TYPE_INSERT);
        }
    }

    /**
     * Initialize the WindowEvent table by creating {@link WindowProcessor} to handle the events.
     *
     * @param tableMap         map of {@link Table}s
     * @param eventWindowMap   map of EventWindows
     * @param windowName       name of the query window belongs to.
     * @param findToBeExecuted will find will be executed on the window.
     */
    public void init(Map<String, Table> tableMap, Map<String, Window> eventWindowMap, String windowName,
                     boolean findToBeExecuted) {
        if (this.windowProcessor != null) {
            return;
        }

        // Create and initialize MetaStreamEvent
        MetaStreamEvent metaStreamEvent = new MetaStreamEvent();
        metaStreamEvent.addInputDefinition(windowDefinition);
        metaStreamEvent.setEventType(MetaStreamEvent.EventType.WINDOW);
        for (Attribute attribute : windowDefinition.getAttributeList()) {
            metaStreamEvent.addOutputData(attribute);
        }

        this.streamEventFactory = new StreamEventFactory(metaStreamEvent);
        StreamEventCloner streamEventCloner = new StreamEventCloner(metaStreamEvent, this.streamEventFactory);
        OutputStream.OutputEventType outputEventType = windowDefinition.getOutputEventType();
        boolean outputExpectsExpiredEvents = outputEventType != OutputStream.OutputEventType.CURRENT_EVENTS;

        SiddhiQueryContext siddhiQueryContext = new SiddhiQueryContext(siddhiAppContext, windowName);
        WindowProcessor internalWindowProcessor = (WindowProcessor) SingleInputStreamParser.generateProcessor
                (windowDefinition.getWindow(), metaStreamEvent, new ArrayList<VariableExpressionExecutor>(),
                        tableMap, false,
                        outputExpectsExpiredEvents, findToBeExecuted, siddhiQueryContext);
        internalWindowProcessor.setStreamEventCloner(streamEventCloner);
        internalWindowProcessor.constructStreamEventPopulater(metaStreamEvent, 0);

        EntryValveProcessor entryValveProcessor = null;
        if (internalWindowProcessor instanceof SchedulingProcessor) {
            entryValveProcessor = new EntryValveProcessor(this.siddhiAppContext);
            Scheduler scheduler = SchedulerParser.parse(entryValveProcessor, siddhiQueryContext);
            scheduler.init(this.lockWrapper, windowName);
            scheduler.setStreamEventFactory(streamEventFactory);
            ((SchedulingProcessor) internalWindowProcessor).setScheduler(scheduler);
        }
        if (entryValveProcessor != null) {
            entryValveProcessor.setToLast(internalWindowProcessor);
            this.windowProcessor = entryValveProcessor;
        } else {
            this.windowProcessor = internalWindowProcessor;
        }

        // StreamPublishProcessor must be the last in chain so that it can publish the events to StreamJunction
        this.windowProcessor.setToLast(new StreamPublishProcessor(outputEventType));
        this.internalWindowProcessor = internalWindowProcessor;
    }


    /**
     * Set Publisher to which the the output events from internal window have to be sent.
     *
     * @param publisher output publisher
     */
    public void setPublisher(StreamJunction.Publisher publisher) {
        this.outputPublisher = publisher;
    }

    /**
     * Return the {@link WindowDefinition} used to construct this Window.
     *
     * @return the window definition
     */
    public WindowDefinition getWindowDefinition() {
        return this.windowDefinition;
    }

    /**
     * Add the given ComplexEventChunk to the Window.
     *
     * @param complexEventChunk the event chunk to be added
     */
    public void add(ComplexEventChunk complexEventChunk) {
        try {
            this.lockWrapper.lock();
            complexEventChunk.reset();

            // Convert all events to StreamEvent because StateEvents can be passed if directly received from a join
            ComplexEvent complexEvents = complexEventChunk.getFirst();
            StreamEvent firstEvent = streamEventFactory.newInstance();
            eventConverter.convertComplexEvent(complexEvents, firstEvent);
            StreamEvent currentEvent = firstEvent;
            complexEvents = complexEvents.getNext();
            int numberOfEvents = 0;
            while (complexEvents != null) {
                numberOfEvents++;
                StreamEvent nextEvent = streamEventFactory.newInstance();
                eventConverter.convertComplexEvent(complexEvents, nextEvent);
                currentEvent.setNext(nextEvent);
                currentEvent = nextEvent;
                complexEvents = complexEvents.getNext();
            }

            try {
                if (throughputTrackerInsert != null &&
                        Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
                    throughputTrackerInsert.eventsIn(numberOfEvents);
                    latencyTrackerInsert.markIn();
                }
                // Send to the window windowProcessor
                windowProcessor.process(new ComplexEventChunk<>(firstEvent, currentEvent));
            } finally {
                if (throughputTrackerInsert != null &&
                        Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
                    latencyTrackerInsert.markOut();
                }
            }
        } finally {
            this.lockWrapper.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        try {
            if (throughputTrackerFind != null && Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
                throughputTrackerFind.eventIn();
                latencyTrackerFind.markIn();
            }
            return ((FindableProcessor) this.internalWindowProcessor).find(matchingEvent, compiledCondition);
        } finally {
            if (throughputTrackerFind != null && Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
                latencyTrackerFind.markOut();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompiledCondition compileCondition(Expression condition, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, SiddhiQueryContext siddhiQueryContext) {
        if (this.internalWindowProcessor instanceof FindableProcessor) {
            return ((FindableProcessor) this.internalWindowProcessor).compileCondition(condition,
                    matchingMetaInfoHolder, variableExpressionExecutors, tableMap, siddhiQueryContext);
        } else {
            throw new OperationNotSupportedException("Cannot construct finder for the window " + this
                    .windowDefinition.getWindow());
        }

    }

    public LockWrapper getLock() {
        return lockWrapper;
    }

    public ProcessingMode getProcessingMode() {
        return internalWindowProcessor.getProcessingMode();
    }

    public boolean isStateful() {
        return internalWindowProcessor.isStateful();
    }

    /**
     * PublisherProcessor receives events from the last window processor of Window,
     * filter them depending on user defined output type and publish them to the stream junction.
     */
    private class StreamPublishProcessor implements Processor {
        /**
         * Allow current events.
         */
        private final boolean allowCurrentEvents;

        /**
         * Allow expired events.
         */
        private final boolean allowExpiredEvents;

        /**
         * User preference of output event type. Stored for cloning purpose.
         */
        private final OutputStream.OutputEventType outputEventType;


        StreamPublishProcessor(OutputStream.OutputEventType outputEventType) {
            this.outputEventType = outputEventType;
            this.allowCurrentEvents = (outputEventType == OutputStream.OutputEventType.CURRENT_EVENTS ||
                    outputEventType == OutputStream.OutputEventType.ALL_EVENTS);
            this.allowExpiredEvents = (outputEventType == OutputStream.OutputEventType.EXPIRED_EVENTS ||
                    outputEventType == OutputStream.OutputEventType.ALL_EVENTS);
        }

        public void process(ComplexEventChunk complexEventChunk) {
            if (throughputTrackerInsert != null &&
                    Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
                latencyTrackerInsert.markOut();
            }
            // Filter the events depending on user defined output type.
            // if(allowCurrentEvents && allowExpiredEvents)
            complexEventChunk.reset();
            while (complexEventChunk.hasNext()) {
                ComplexEvent event = complexEventChunk.next();
                if (((event.getType() != StreamEvent.Type.CURRENT || !allowCurrentEvents) && (event.getType() !=
                        StreamEvent.Type.EXPIRED || !allowExpiredEvents))) {
                    complexEventChunk.remove();
                }
            }
            complexEventChunk.reset();
            if (complexEventChunk.hasNext()) {
                // Publish the events
                outputPublisher.send(complexEventChunk.getFirst());
            }
        }

        @Override
        public void process(List<ComplexEventChunk> complexEventChunks) {
            ComplexEventChunk complexEventChunk = new ComplexEventChunk();
            for (ComplexEventChunk streamEventChunk : complexEventChunks) {
                complexEventChunk.addAll(streamEventChunk);
            }
            process(complexEventChunk);
        }

        public Processor getNextProcessor() {
            return null;
        }


        public void setNextProcessor(Processor processor) {
            // Do nothing
        }


        public void setToLast(Processor processor) {
            // Do nothing
        }

    }
}
