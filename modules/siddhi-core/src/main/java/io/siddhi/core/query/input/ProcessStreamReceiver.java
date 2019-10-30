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
package io.siddhi.core.query.input;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.debugger.SiddhiDebugger;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.Event;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.event.stream.converter.StreamEventConverter;
import io.siddhi.core.event.stream.converter.StreamEventConverterFactory;
import io.siddhi.core.query.input.stream.state.PreStateProcessor;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.stream.StreamJunction;
import io.siddhi.core.util.lock.LockWrapper;
import io.siddhi.core.util.statistics.LatencyTracker;
import io.siddhi.core.util.statistics.metrics.Level;

import java.util.ArrayList;
import java.util.List;

/**
 * Parent implementation for all process stream receivers(Multi/Single/State). Any newly written process stream
 * receivers should extend this. ProcessStreamReceivers are the entry point to Siddhi queries.
 */
public class ProcessStreamReceiver implements StreamJunction.Receiver {

    protected final SiddhiQueryContext siddhiQueryContext;
    protected String streamId;
    protected Processor next;
    protected List<PreStateProcessor> stateProcessorsForStream = new ArrayList<PreStateProcessor>();
    protected int stateProcessorsForStreamSize;
    protected LockWrapper lockWrapper;
    protected List<PreStateProcessor> allStateProcessors = new ArrayList<PreStateProcessor>();
    protected int allStateProcessorsSize;
    private StreamEventConverter streamEventConverter;
    private MetaStreamEvent metaStreamEvent;
    private StreamEventFactory streamEventFactory;
    private SiddhiDebugger siddhiDebugger;

    public ProcessStreamReceiver(String streamId,
                                 SiddhiQueryContext siddhiQueryContext) {
        this.streamId = streamId;
        this.siddhiQueryContext = siddhiQueryContext;
    }

    @Override
    public String getStreamId() {
        return streamId;
    }

    public void setSiddhiDebugger(SiddhiDebugger siddhiDebugger) {
        this.siddhiDebugger = siddhiDebugger;
    }

    private void process(ComplexEventChunk<StreamEvent> streamEventChunk) {
        if (lockWrapper != null) {
            lockWrapper.lock();
        }
        try {
            LatencyTracker latencyTracker = siddhiQueryContext.getLatencyTracker();
            if (Level.BASIC.compareTo(siddhiQueryContext.getSiddhiAppContext().getRootMetricsLevel()) <= 0 &&
                    latencyTracker != null) {
                try {
                    latencyTracker.markIn();
                    processAndClear(streamEventChunk);
                } finally {
                    latencyTracker.markOut();
                }
            } else {
                processAndClear(streamEventChunk);
            }
        } finally {
            if (lockWrapper != null) {
                lockWrapper.unlock();
            }
        }
    }

    @Override
    public void receive(ComplexEvent complexEvents) {
        if (siddhiDebugger != null) {
            siddhiDebugger.checkBreakPoint(siddhiQueryContext.getName(),
                    SiddhiDebugger.QueryTerminal.IN, complexEvents);
        }
        StreamEvent firstEvent = streamEventFactory.newInstance();
        streamEventConverter.convertComplexEvent(complexEvents, firstEvent);
        StreamEvent currentEvent = firstEvent;
        complexEvents = complexEvents.getNext();
        while (complexEvents != null) {
            StreamEvent nextEvent = streamEventFactory.newInstance();
            streamEventConverter.convertComplexEvent(complexEvents, nextEvent);
            currentEvent.setNext(nextEvent);
            currentEvent = nextEvent;
            complexEvents = complexEvents.getNext();
        }
        process(new ComplexEventChunk<StreamEvent>(firstEvent, currentEvent));
    }

    @Override
    public void receive(Event event) {
        if (event != null) {
            StreamEvent newEvent = streamEventFactory.newInstance();
            streamEventConverter.convertEvent(event, newEvent);
            if (siddhiDebugger != null) {
                siddhiDebugger.checkBreakPoint(siddhiQueryContext.getName(),
                        SiddhiDebugger.QueryTerminal.IN, newEvent);
            }
            process(new ComplexEventChunk<StreamEvent>(newEvent, newEvent));
        }
    }

    @Override
    public void receive(Event[] events) {
        StreamEvent firstEvent = streamEventFactory.newInstance();
        streamEventConverter.convertEvent(events[0], firstEvent);
        StreamEvent currentEvent = firstEvent;
        for (int i = 1, eventsLength = events.length; i < eventsLength; i++) {
            StreamEvent nextEvent = streamEventFactory.newInstance();
            streamEventConverter.convertEvent(events[i], nextEvent);
            currentEvent.setNext(nextEvent);
            currentEvent = nextEvent;
        }
        if (siddhiDebugger != null) {
            siddhiDebugger.checkBreakPoint(siddhiQueryContext.getName(), SiddhiDebugger.QueryTerminal.IN, firstEvent);
        }
        process(new ComplexEventChunk<StreamEvent>(firstEvent, currentEvent));
    }

    @Override
    public void receive(List<Event> events) {
        StreamEvent firstEvent = null;
        StreamEvent currentEvent = null;
        for (Event event : events) {
            StreamEvent nextEvent = streamEventFactory.newInstance();
            streamEventConverter.convertEvent(event, nextEvent);
            if (firstEvent == null) {
                firstEvent = nextEvent;
            } else {
                currentEvent.setNext(nextEvent);
            }
            currentEvent = nextEvent;

        }
        if (siddhiDebugger != null) {
            siddhiDebugger.checkBreakPoint(siddhiQueryContext.getName(), SiddhiDebugger.QueryTerminal.IN, firstEvent);
        }
        process(new ComplexEventChunk<StreamEvent>(firstEvent, currentEvent));
    }

    @Override
    public void receive(long timestamp, Object[] data) {
        StreamEvent newEvent = streamEventFactory.newInstance();
        streamEventConverter.convertData(timestamp, data, newEvent);
        // Send to debugger
        if (siddhiDebugger != null) {
            siddhiDebugger.checkBreakPoint(siddhiQueryContext.getName(),
                    SiddhiDebugger.QueryTerminal.IN, newEvent);
        }
        process(new ComplexEventChunk<StreamEvent>(newEvent, newEvent));
    }

    protected void processAndClear(ComplexEventChunk<StreamEvent> streamEventChunk) {
        next.process(streamEventChunk);
        streamEventChunk.clear();
    }

    public void setMetaStreamEvent(MetaStreamEvent metaStreamEvent) {
        this.metaStreamEvent = metaStreamEvent;
    }

    public boolean toStream() {
        return metaStreamEvent.getEventType() == MetaStreamEvent.EventType.DEFAULT ||
                metaStreamEvent.getEventType() == MetaStreamEvent.EventType.WINDOW;
    }

    public void setNext(Processor next) {
        this.next = next;
    }

    public void setStreamEventFactory(StreamEventFactory streamEventFactory) {
        this.streamEventFactory = streamEventFactory;
    }

    public void setLockWrapper(LockWrapper lockWrapper) {
        this.lockWrapper = lockWrapper;
    }

    public void init() {
        streamEventConverter = StreamEventConverterFactory.constructEventConverter(metaStreamEvent);
    }

    public void addStatefulProcessorForStream(PreStateProcessor stateProcessor) {
        stateProcessorsForStream.add(stateProcessor);
        stateProcessorsForStreamSize = stateProcessorsForStream.size();
    }

    public void setAllStatefulProcessors(List<PreStateProcessor> allStateProcessors) {
        this.allStateProcessors = allStateProcessors;
        this.allStateProcessorsSize = allStateProcessors.size();

    }
}
