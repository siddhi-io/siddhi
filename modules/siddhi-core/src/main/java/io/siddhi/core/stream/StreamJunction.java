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

package io.siddhi.core.stream;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.Event;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.event.stream.converter.FaultStreamEventConverter;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.input.InputProcessor;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.error.handler.model.ErroneousEvent;
import io.siddhi.core.util.error.handler.util.ErrorOccurrence;
import io.siddhi.core.util.error.handler.util.ErrorStoreHelper;
import io.siddhi.core.util.event.handler.EventExchangeHolder;
import io.siddhi.core.util.event.handler.EventExchangeHolderFactory;
import io.siddhi.core.util.event.handler.StreamHandler;
import io.siddhi.core.util.parser.helper.QueryParserHelper;
import io.siddhi.core.util.statistics.EventBufferHolder;
import io.siddhi.core.util.statistics.ThroughputTracker;
import io.siddhi.core.util.statistics.metrics.Level;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.exception.DuplicateAnnotationException;
import io.siddhi.query.api.util.AnnotationHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.beans.ExceptionListener;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Stream Junction is the place where streams are collected and distributed. There will be an Stream Junction per
 * evey event stream. {@link StreamJunction.Publisher} can be used to publish events to the junction and
 * {@link StreamJunction.Receiver} can be used to receive events from Stream Junction. Stream Junction will hold the
 * events till they are consumed by registered Receivers.
 */
public class StreamJunction implements EventBufferHolder {
    private static final Logger log = LogManager.getLogger(StreamJunction.class);
    private final SiddhiAppContext siddhiAppContext;
    private final StreamDefinition streamDefinition;
    private int batchSize;
    private int workers = -1;
    private int bufferSize;
    private List<Receiver> receivers = new LinkedList<>();
    private List<Publisher> publishers = new LinkedList<>();
    private ExecutorService executorService;
    private boolean async = false;
    private Disruptor<EventExchangeHolder> disruptor;
    private RingBuffer<EventExchangeHolder> ringBuffer;
    private ThroughputTracker throughputTracker = null;
    private boolean isTraceEnabled;
    private StreamJunction faultStreamJunction = null;
    private FaultStreamEventConverter faultStreamEventConverter = null;
    private OnErrorAction onErrorAction = OnErrorAction.LOG;
    private ExceptionListener exceptionListener;

    public StreamJunction(StreamDefinition streamDefinition, ExecutorService executorService, int bufferSize,
                          StreamJunction faultStreamJunction, SiddhiAppContext siddhiAppContext) {
        this.streamDefinition = streamDefinition;
        this.bufferSize = bufferSize;
        this.batchSize = bufferSize;
        this.executorService = executorService;
        this.siddhiAppContext = siddhiAppContext;
        if (siddhiAppContext.getStatisticsManager() != null) {
            this.throughputTracker = QueryParserHelper.createThroughputTracker(siddhiAppContext,
                    streamDefinition.getId(),
                    SiddhiConstants.METRIC_INFIX_STREAMS, null);
        }
        this.faultStreamJunction = faultStreamJunction;
        if (faultStreamJunction != null) {
            StreamDefinition faultStreamDefinition = faultStreamJunction.getStreamDefinition();
            StreamEventFactory faultStreamEventFactory = new StreamEventFactory(0, 0,
                    faultStreamDefinition.getAttributeList().size());
            faultStreamEventConverter = new FaultStreamEventConverter(faultStreamEventFactory);
        }
        try {
            Annotation asyncAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_ASYNC,
                    streamDefinition.getAnnotations());
            if (asyncAnnotation != null) {
                async = true;
                String bufferSizeString = asyncAnnotation.getElement(SiddhiConstants.ANNOTATION_ELEMENT_BUFFER_SIZE);
                if (bufferSizeString != null) {
                    this.bufferSize = Integer.parseInt(bufferSizeString);
                }
                String workersString = asyncAnnotation.getElement(SiddhiConstants.ANNOTATION_ELEMENT_WORKERS);
                if (workersString != null) {
                    this.workers = Integer.parseInt(workersString);
                    if (workers <= 0) {
                        throw new SiddhiAppCreationException("Annotation element '" +
                                SiddhiConstants.ANNOTATION_ELEMENT_WORKERS + "' cannot be negative or zero, " +
                                "but found, '" + workers + "'.", asyncAnnotation.getQueryContextStartIndex(),
                                asyncAnnotation.getQueryContextEndIndex(), siddhiAppContext.getName(),
                                siddhiAppContext.getSiddhiAppString());
                    }
                }
                String batchSizeString = asyncAnnotation.getElement(SiddhiConstants.ANNOTATION_ELEMENT_MAX_BATCH_SIZE);
                if (batchSizeString != null) {
                    this.batchSize = Integer.parseInt(batchSizeString);
                    if (batchSize <= 0) {
                        throw new SiddhiAppCreationException("Annotation element '" +
                                SiddhiConstants.ANNOTATION_ELEMENT_MAX_BATCH_SIZE + "' cannot be negative or zero, " +
                                "but found, '" + batchSize + "'.", asyncAnnotation.getQueryContextStartIndex(),
                                asyncAnnotation.getQueryContextEndIndex(), siddhiAppContext.getName(),
                                siddhiAppContext.getSiddhiAppString());
                    }
                }
            }
            Annotation onErrorAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_ON_ERROR,
                    streamDefinition.getAnnotations());
            if (onErrorAnnotation != null) {
                this.onErrorAction = OnErrorAction.valueOf(onErrorAnnotation
                        .getElement(SiddhiConstants.ANNOTATION_ELEMENT_ACTION).toUpperCase());
            }
        } catch (DuplicateAnnotationException e) {
            throw new DuplicateAnnotationException(e.getMessageWithOutContext() + " for the same Stream " +
                    streamDefinition.getId(), e, e.getQueryContextStartIndex(), e.getQueryContextEndIndex(),
                    siddhiAppContext.getName(), siddhiAppContext.getSiddhiAppString());
        }
        isTraceEnabled = log.isTraceEnabled();
    }

    public void sendEvent(ComplexEvent complexEvent) {
        if (isTraceEnabled) {
            log.trace("Event is received by streamJunction " + this);
        }
        ComplexEvent complexEventList = complexEvent;
        if (disruptor != null) {
            while (complexEventList != null) {
                if (throughputTracker != null && Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
                    throughputTracker.eventIn();
                }
                long sequenceNo = ringBuffer.next();
                try {
                    EventExchangeHolder eventExchangeHolder = ringBuffer.get(sequenceNo);
                    eventExchangeHolder.getEvent().copyFrom(complexEventList);
                    eventExchangeHolder.getAndSetIsProcessed(false);
                } finally {
                    ringBuffer.publish(sequenceNo);
                }
                complexEventList = complexEventList.getNext();
            }
        } else {
            if (throughputTracker != null && Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
                int messageCount = 0;
                while (complexEventList != null) {
                    messageCount++;
                    complexEventList = complexEventList.getNext();
                }
                throughputTracker.eventsIn(messageCount);
            }
            for (Receiver receiver : receivers) {
                receiver.receive(complexEvent);
            }
        }
    }

    public void sendEvent(Event event) {
        if (throughputTracker != null && Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
            throughputTracker.eventIn();
        }
        if (isTraceEnabled) {
            log.trace(event + " event is received by streamJunction " + this);
        }
        if (disruptor != null) {
            long sequenceNo = ringBuffer.next();
            try {
                EventExchangeHolder eventExchangeHolder = ringBuffer.get(sequenceNo);
                eventExchangeHolder.getEvent().copyFrom(event);
                eventExchangeHolder.getAndSetIsProcessed(false);
            } finally {
                ringBuffer.publish(sequenceNo);
            }
        } else {
            for (Receiver receiver : receivers) {
                receiver.receive(event);
            }
        }
    }

    private void sendEvent(Event[] events) {
        if (throughputTracker != null && Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
            throughputTracker.eventsIn(events.length);
        }
        if (isTraceEnabled) {
            log.trace("Event is received by streamJunction " + this);
        }
        if (disruptor != null) {
            for (Event event : events) {   // Todo : optimize for arrays
                long sequenceNo = ringBuffer.next();
                try {
                    EventExchangeHolder eventExchangeHolder = ringBuffer.get(sequenceNo);
                    eventExchangeHolder.getEvent().copyFrom(event);
                    eventExchangeHolder.getAndSetIsProcessed(false);
                } finally {
                    ringBuffer.publish(sequenceNo);
                }
            }
        } else {
            for (Receiver receiver : receivers) {
                receiver.receive(events);
            }
        }
    }

    private void sendEvent(List<Event> events) {
        if (isTraceEnabled) {
            log.trace("Event is received by streamJunction " + this);
        }
        if (disruptor != null) {
            for (Event event : events) {   // Todo : optimize for arrays
                long sequenceNo = ringBuffer.next();
                try {
                    EventExchangeHolder eventExchangeHolder = ringBuffer.get(sequenceNo);
                    eventExchangeHolder.getEvent().copyFrom(event);
                    eventExchangeHolder.getAndSetIsProcessed(false);
                } finally {
                    ringBuffer.publish(sequenceNo);
                }
            }
        } else {
            for (Receiver receiver : receivers) {
                receiver.receive(events.toArray(new Event[events.size()]));
            }
        }
    }

    private void sendData(long timeStamp, Object[] data) {
        if (throughputTracker != null && Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
            throughputTracker.eventIn();
        }
        if (disruptor != null) {
            long sequenceNo = ringBuffer.next();
            try {
                EventExchangeHolder eventExchangeHolder = ringBuffer.get(sequenceNo);
                eventExchangeHolder.getAndSetIsProcessed(false);
                eventExchangeHolder.getEvent().setTimestamp(timeStamp);
                eventExchangeHolder.getEvent().setIsExpired(false);
                System.arraycopy(data, 0, eventExchangeHolder.getEvent().getData(), 0, data.length);
            } finally {
                ringBuffer.publish(sequenceNo);
            }
        } else {
            for (Receiver receiver : receivers) {
                receiver.receive(timeStamp, data);
            }
        }
    }

    /**
     * Create and start disruptor based on annotations given in the streamDefinition.
     */
    public void startProcessing() {
        this.exceptionListener = siddhiAppContext.getRuntimeExceptionListener();
        if (!receivers.isEmpty() && async) {
            for (Constructor constructor : Disruptor.class.getConstructors()) {
                if (constructor.getParameterTypes().length == 5) {      // If new disruptor classes available
                    ProducerType producerType = ProducerType.MULTI;
                    disruptor = new Disruptor<EventExchangeHolder>(
                            new EventExchangeHolderFactory(streamDefinition.getAttributeList().size()),
                            bufferSize, executorService, producerType,
                            new BlockingWaitStrategy());
                    disruptor.handleExceptionsWith(siddhiAppContext.getDisruptorExceptionHandler());
                    break;
                }
            }
            if (disruptor == null) {
                disruptor = new Disruptor<EventExchangeHolder>(
                        new EventExchangeHolderFactory(streamDefinition.getAttributeList().size()),
                        bufferSize, executorService);
                disruptor.handleExceptionsWith(siddhiAppContext.getDisruptorExceptionHandler());
            }
            if (workers > 0) {
                for (int i = 0; i < workers; i++) {
                    disruptor.handleEventsWith(new StreamHandler(receivers, batchSize, streamDefinition.getId(),
                            siddhiAppContext.getName(), faultStreamJunction, onErrorAction, exceptionListener));
                }
            } else {
                disruptor.handleEventsWith(new StreamHandler(receivers, batchSize, streamDefinition.getId(),
                        siddhiAppContext.getName(), faultStreamJunction, onErrorAction, exceptionListener));
            }
            ringBuffer = disruptor.start();
        } else {
            for (Receiver receiver : receivers) {
                if (receiver instanceof StreamCallback) {
                    ((StreamCallback) receiver).startProcessing();
                }
            }
        }
    }

    public void stopProcessing() {
        if (disruptor != null) {
            disruptor.shutdown();
        } else {
            for (Receiver receiver : receivers) {
                if (receiver instanceof StreamCallback) {
                    ((StreamCallback) receiver).stopProcessing();
                }
            }
        }
    }

    public Publisher constructPublisher() {
        Publisher publisher = new Publisher();
        publisher.setStreamJunction(this);
        publishers.add(publisher);
        return publisher;
    }

    public void subscribe(Receiver receiver) {
        // To have reverse order at the sequence/pattern processors.
        if (!receivers.contains(receiver)) {
            receivers.add(receiver);
        }
    }

    public void unsubscribe(StreamCallback streamCallback) {
        List<Receiver> newReceivers = new LinkedList<>(receivers);
        newReceivers.remove(streamCallback);
        receivers = newReceivers;
    }

    public String getStreamId() {
        return streamDefinition.getId();
    }

    public StreamDefinition getStreamDefinition() {
        return streamDefinition;
    }

    @Override
    public long getBufferedEvents() {
        if (disruptor != null) {
            return disruptor.getBufferSize() - disruptor.getRingBuffer().remainingCapacity();
        }
        return 0L;
    }

    @Override
    public boolean containsBufferedEvents() {
        return (!receivers.isEmpty() && async);
    }

    public void handleError(Object event, Exception e) {
        if (exceptionListener != null) {
            exceptionListener.exceptionThrown(e);
        }
        switch (onErrorAction) {
            case LOG:
                log.error("Error in '" + siddhiAppContext.getName() + "' after consuming events "
                        + "from Stream '" + streamDefinition.getId() + "', " + e.getMessage()
                        + ". Hence, dropping event '" + event.toString() + "'", e);
                break;
            case STREAM:
                if (faultStreamJunction != null) {
                    StreamEvent streamEvent = null;
                    if (event instanceof ComplexEvent) {
                        streamEvent = faultStreamEventConverter.convert((ComplexEvent) event, e);
                        faultStreamJunction.sendEvent(streamEvent);
                    } else if (event instanceof Event) {
                        streamEvent = faultStreamEventConverter.convert((Event) event, e);
                        faultStreamJunction.sendEvent(streamEvent);
                    } else if (event instanceof Event[]) {
                        streamEvent = faultStreamEventConverter.convert((Event[]) event, e);
                        faultStreamJunction.sendEvent(streamEvent);
                    } else if (event instanceof List) {
                        streamEvent = faultStreamEventConverter.convert((List<Event>) event, e);
                        faultStreamJunction.sendEvent(streamEvent);
                    }
                } else {
                    log.error("Error in SiddhiApp '" + siddhiAppContext.getName() +
                            "' after consuming events from Stream '" + streamDefinition.getId()
                            + "', " + e.getMessage() + ". Siddhi Fault Stream for '" + streamDefinition.getId()
                            + "' is not defined. " + "Hence, dropping event '" + event.toString() + "'", e);
                }
                break;
            case STORE:
                ErroneousEvent erroneousEvent =
                        new ErroneousEvent(event, e, "Error in SiddhiApp '" + siddhiAppContext.getName() +
                                "' after consuming events from Stream '" + streamDefinition.getId()
                                + "', " + e.getMessage() + ". Siddhi Fault Stream for '" + streamDefinition.getId()
                                + "' is not defined. " + "Hence, dropping event '" + event.toString() + "'");
                ErrorStoreHelper.storeErroneousEvent(siddhiAppContext.getSiddhiContext().getErrorStore(),
                        ErrorOccurrence.STORE_ON_STREAM_ERROR, siddhiAppContext.getName(), erroneousEvent,
                        streamDefinition.getId());
                break;
            default:
                break;
        }
    }

    public void handleError(long timeStamp, Object[] data, Exception e) {
        if (exceptionListener != null) {
            exceptionListener.exceptionThrown(e);
        }
        switch (onErrorAction) {
            case LOG:
                log.error("Error in '" + siddhiAppContext.getName() + "' after consuming events "
                        + "from Stream '" + streamDefinition.getId() + "', " + e.getMessage()
                        + ". Hence, dropping event '" + Arrays.toString(data) + "'", e);
                break;
            case STREAM:
                if (faultStreamJunction != null) {
                    StreamEvent streamEvent = faultStreamEventConverter.convert(timeStamp, data, e);
                    faultStreamJunction.sendEvent(streamEvent);
                } else {
                    log.error("Error in SiddhiApp '" + siddhiAppContext.getName() +
                            "' after consuming events from Stream '" + streamDefinition.getId()
                            + "', " + e.getMessage() + ". Siddhi Fault Stream for '" + streamDefinition.getId()
                            + "' is not defined. " + "Hence, dropping data '" + Arrays.toString(data) + "'", e);
                }
                break;
            case STORE:
                StreamEvent streamEvent = faultStreamEventConverter.convert(timeStamp, data, e);
                ErroneousEvent erroneousEvent =
                        new ErroneousEvent(streamEvent, e, "Error in SiddhiApp '" + siddhiAppContext.getName() +
                                "' after consuming events from Stream '" + streamDefinition.getId()
                                + "', " + e.getMessage() + ". Siddhi Fault Stream for '" + streamDefinition.getId()
                                + "' is not defined. " + "Hence, dropping data '" + Arrays.toString(data) + "'");
                ErrorStoreHelper.storeErroneousEvent(siddhiAppContext.getSiddhiContext().getErrorStore(),
                        ErrorOccurrence.STORE_ON_STREAM_ERROR, siddhiAppContext.getName(), erroneousEvent,
                        streamDefinition.getId());
                break;
            default:
                break;
        }
    }

    /**
     * Different Type of On Error Actions
     */
    public enum OnErrorAction {
        LOG,
        STREAM,
        STORE
    }

    /**
     * Interface to be implemented by all receivers who need to subscribe to Stream Junction and receive events.
     */
    public interface Receiver {

        String getStreamId();

        void receive(ComplexEvent complexEvent);

        void receive(Event event);

        void receive(List<Event> events);

        void receive(long timeStamp, Object[] data);

        void receive(Event[] events);
    }

    /**
     * Interface to be implemented to send events into the Stream Junction.
     */
    public class Publisher implements InputProcessor {

        private StreamJunction streamJunction;

        public void setStreamJunction(StreamJunction streamJunction) {
            this.streamJunction = streamJunction;
        }

        public void send(ComplexEvent complexEvent) {
            try {
                streamJunction.sendEvent(complexEvent);
            } catch (Exception e) {
                streamJunction.handleError(complexEvent, e);
            }
        }

        @Override
        public void send(Event event, int streamIndex) {
            try {
                streamJunction.sendEvent(event);
            } catch (Exception e) {
                streamJunction.handleError(event, e);
            }
        }

        @Override
        public void send(Event[] events, int streamIndex) {
            try {
                streamJunction.sendEvent(events);
            } catch (Exception e) {
                streamJunction.handleError(events, e);
            }
        }

        @Override
        public void send(List<Event> events, int streamIndex) {
            try {
                streamJunction.sendEvent(events);
            } catch (Exception e) {
                streamJunction.handleError(events, e);
            }
        }

        @Override
        public void send(long timeStamp, Object[] data, int streamIndex) {
            try {
                streamJunction.sendData(timeStamp, data);
            } catch (Exception e) {
                streamJunction.handleError(timeStamp, data, e);
            }
        }

        public String getStreamId() {
            return streamJunction.getStreamId();
        }

    }
}
