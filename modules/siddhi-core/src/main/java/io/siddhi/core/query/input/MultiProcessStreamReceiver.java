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
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.Event;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.event.stream.converter.StreamEventConverter;
import io.siddhi.core.event.stream.converter.StreamEventConverterFactory;
import io.siddhi.core.query.output.ratelimit.OutputRateLimiter;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.util.statistics.LatencyTracker;

import java.util.ArrayList;
import java.util.List;

/**
 * {StreamJunction.Receiver} implementation to receive events to be fed into multi
 * stream processors which consume multiple streams.
 */
public class MultiProcessStreamReceiver extends ProcessStreamReceiver {

    private static ThreadLocal<ReturnEventHolder> multiProcessReturn = new ThreadLocal<>();
    private final Object patternSyncObject;
    protected Processor[] nextProcessors;
    protected int[] eventSequence;
    protected OutputRateLimiter outputRateLimiter;
    private MetaStreamEvent[] metaStreamEvents;
    private StreamEventFactory[] streamEventFactorys;
    private StreamEventConverter[] streamEventConverters;


    public MultiProcessStreamReceiver(String streamId, int processCount,
                                      Object patternSyncObject, SiddhiQueryContext siddhiQueryContext) {
        super(streamId, siddhiQueryContext);
        nextProcessors = new Processor[processCount];
        metaStreamEvents = new MetaStreamEvent[processCount];
        streamEventFactorys = new StreamEventFactory[processCount];
        streamEventConverters = new StreamEventConverter[processCount];
        eventSequence = new int[processCount];
        this.patternSyncObject = patternSyncObject;
        for (int i = 0; i < eventSequence.length; i++) {
            eventSequence[i] = i;
        }
    }

    public static ThreadLocal<ReturnEventHolder> getMultiProcessReturn() {
        return multiProcessReturn;
    }

    private void process(int eventSequence, StreamEvent newEvent) {
        if (lockWrapper != null) {
            lockWrapper.lock();
        }
        try {
            LatencyTracker latencyTracker = siddhiQueryContext.getLatencyTracker();
            if (latencyTracker != null) {
                try {
                    latencyTracker.markIn();
                    processAndClear(eventSequence, newEvent);
                } finally {
                    latencyTracker.markOut();
                }
            } else {
                processAndClear(eventSequence, newEvent);
            }
        } finally {
            if (lockWrapper != null) {
                lockWrapper.unlock();
            }
        }
    }

    @Override
    public void receive(ComplexEvent complexEvent) {
        ComplexEvent aComplexEvent = complexEvent;
        List<ReturnEventHolder> returnEventHolderList = new ArrayList<>(eventSequence.length);
        synchronized (patternSyncObject) {
            while (aComplexEvent != null) {
                try {
                    multiProcessReturn.set(new ReturnEventHolder());
                    stabilizeStates(aComplexEvent.getTimestamp());
                    for (int anEventSequence : eventSequence) {
                        StreamEventConverter aStreamEventConverter = streamEventConverters[anEventSequence];
                        StreamEventFactory aStreamEventFactory = streamEventFactorys[anEventSequence];
                        StreamEvent newEvent = aStreamEventFactory.newInstance();
                        aStreamEventConverter.convertComplexEvent(aComplexEvent, newEvent);
                        process(anEventSequence, newEvent);
                        if (multiProcessReturn.get() != null &&
                                multiProcessReturn.get().complexEventChunk != null) {
                            returnEventHolderList.add(multiProcessReturn.get());
                            multiProcessReturn.set(new ReturnEventHolder());
                        }
                    }
                } finally {
                    multiProcessReturn.set(null);
                }
                aComplexEvent = aComplexEvent.getNext();
            }
            for (ReturnEventHolder returnEventHolder : returnEventHolderList) {
                outputRateLimiter.sendToCallBacks(returnEventHolder.complexEventChunk);
            }
        }

    }

    @Override
    public void receive(Event event) {
        List<ReturnEventHolder> returnEventHolderList = new ArrayList<>(eventSequence.length);
        synchronized (patternSyncObject) {
            try {
                multiProcessReturn.set(new ReturnEventHolder());

                stabilizeStates(event.getTimestamp());
                for (int anEventSequence : eventSequence) {
                    StreamEventConverter aStreamEventConverter = streamEventConverters[anEventSequence];
                    StreamEventFactory aStreamEventFactory = streamEventFactorys[anEventSequence];
                    StreamEvent newEvent = aStreamEventFactory.newInstance();
                    aStreamEventConverter.convertEvent(event, newEvent);
                    process(anEventSequence, newEvent);
                    if (multiProcessReturn.get() != null &&
                            multiProcessReturn.get().complexEventChunk != null) {
                        returnEventHolderList.add(multiProcessReturn.get());
                        multiProcessReturn.set(new ReturnEventHolder());
                    }
                }
            } finally {
                multiProcessReturn.set(null);
            }
        }
        for (ReturnEventHolder returnEventHolder : returnEventHolderList) {
            outputRateLimiter.sendToCallBacks(returnEventHolder.complexEventChunk);
        }
    }

    @Override
    public void receive(Event[] events) {
        List<ReturnEventHolder> returnEventHolderList = new ArrayList<>(eventSequence.length);
        synchronized (patternSyncObject) {
            for (Event event : events) {
                try {
                    multiProcessReturn.set(new ReturnEventHolder());
                    stabilizeStates(event.getTimestamp());
                    for (int anEventSequence : eventSequence) {
                        StreamEventConverter aStreamEventConverter = streamEventConverters[anEventSequence];
                        StreamEventFactory aStreamEventFactory = streamEventFactorys[anEventSequence];
                        StreamEvent newEvent = aStreamEventFactory.newInstance();
                        aStreamEventConverter.convertEvent(event, newEvent);
                        process(anEventSequence, newEvent);
                        if (multiProcessReturn.get() != null &&
                                multiProcessReturn.get().complexEventChunk != null) {
                            returnEventHolderList.add(multiProcessReturn.get());
                            multiProcessReturn.set(new ReturnEventHolder());
                        }
                    }
                } finally {
                    multiProcessReturn.set(null);
                }
            }
        }
        for (ReturnEventHolder returnEventHolder : returnEventHolderList) {
            outputRateLimiter.sendToCallBacks(returnEventHolder.complexEventChunk);
        }
    }

    @Override
    public void receive(List<Event> events) {
        List<ReturnEventHolder> returnEventHolderList = new ArrayList<>(eventSequence.length);
        synchronized (patternSyncObject) {
            for (Event event : events) {
                try {
                    multiProcessReturn.set(new ReturnEventHolder());
                    stabilizeStates(event.getTimestamp());
                    for (int anEventSequence : eventSequence) {
                        StreamEventConverter aStreamEventConverter = streamEventConverters[anEventSequence];
                        StreamEventFactory aStreamEventFactory = streamEventFactorys[anEventSequence];
                        StreamEvent newEvent = aStreamEventFactory.newInstance();
                        aStreamEventConverter.convertEvent(event, newEvent);
                        process(anEventSequence, newEvent);
                        if (multiProcessReturn.get() != null &&
                                multiProcessReturn.get().complexEventChunk != null) {
                            returnEventHolderList.add(multiProcessReturn.get());
                            multiProcessReturn.set(new ReturnEventHolder());
                        }
                    }
                } finally {
                    multiProcessReturn.set(null);
                }
            }
        }
        for (ReturnEventHolder returnEventHolder : returnEventHolderList) {
            outputRateLimiter.sendToCallBacks(returnEventHolder.complexEventChunk);
        }
    }

    @Override
    public void receive(long timestamp, Object[] data) {
        List<ReturnEventHolder> returnEventHolderList = new ArrayList<>(eventSequence.length);
        synchronized (patternSyncObject) {
            try {
                multiProcessReturn.set(new ReturnEventHolder());
                stabilizeStates(timestamp);
                for (int anEventSequence : eventSequence) {
                    StreamEventConverter aStreamEventConverter = streamEventConverters[anEventSequence];
                    StreamEventFactory aStreamEventFactory = streamEventFactorys[anEventSequence];
                    StreamEvent newEvent = aStreamEventFactory.newInstance();
                    aStreamEventConverter.convertData(timestamp, data, newEvent);
                    process(anEventSequence, newEvent);
                    if (multiProcessReturn.get() != null &&
                            multiProcessReturn.get().complexEventChunk != null) {
                        returnEventHolderList.add(multiProcessReturn.get());
                        multiProcessReturn.set(new ReturnEventHolder());
                    }
                }
            } finally {
                multiProcessReturn.set(null);
            }
        }
        for (ReturnEventHolder returnEventHolder : returnEventHolderList) {
            outputRateLimiter.sendToCallBacks(returnEventHolder.complexEventChunk);
        }
    }

    protected void processAndClear(int processIndex, StreamEvent streamEvent) {
        ComplexEventChunk<StreamEvent> currentStreamEventChunk = new ComplexEventChunk<StreamEvent>(
                streamEvent, streamEvent);
        nextProcessors[processIndex].process(currentStreamEventChunk);
    }

    protected void stabilizeStates(long timestamp) {

    }

    public void setNext(Processor nextProcessor) {
        for (int i = 0, nextLength = nextProcessors.length; i < nextLength; i++) {
            Processor processor = nextProcessors[i];
            if (processor == null) {
                nextProcessors[i] = nextProcessor;
                break;
            }
        }
    }

    public void setMetaStreamEvent(MetaStreamEvent metaStreamEvent) {
        for (int i = 0, nextLength = metaStreamEvents.length; i < nextLength; i++) {
            MetaStreamEvent streamEvent = metaStreamEvents[i];
            if (streamEvent == null) {
                metaStreamEvents[i] = metaStreamEvent;
                break;
            }
        }
    }

    @Override
    public boolean toStream() {
        return metaStreamEvents[0].getEventType() == MetaStreamEvent.EventType.DEFAULT ||
                metaStreamEvents[0].getEventType() == MetaStreamEvent.EventType.WINDOW;
    }

    public void setStreamEventFactory(StreamEventFactory streamEventFactory) {
        for (int i = 0, nextLength = streamEventFactorys.length; i < nextLength; i++) {
            StreamEventFactory eventPool = streamEventFactorys[i];
            if (eventPool == null) {
                streamEventFactorys[i] = streamEventFactory;
                break;
            }
        }
    }

    public void init() {
        for (int i = 0, nextLength = streamEventConverters.length; i < nextLength; i++) {
            StreamEventConverter streamEventConverter = streamEventConverters[i];
            if (streamEventConverter == null) {
                streamEventConverters[i] = StreamEventConverterFactory.constructEventConverter(metaStreamEvents[i]);
                break;
            }
        }
    }

    public void setOutputRateLimiter(OutputRateLimiter outputRateLimiter) {
        this.outputRateLimiter = outputRateLimiter;
    }

    /**
     * Class to hold the events which are differed publishing
     */
    public class ReturnEventHolder {
        ComplexEventChunk complexEventChunk;

        public void setReturnEvents(ComplexEventChunk complexEventChunk) {
            if (this.complexEventChunk == null) {
                this.complexEventChunk = new ComplexEventChunk();
            }
            this.complexEventChunk.add(complexEventChunk.getFirst());
        }
    }
}
