/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.core.util.error.handler.store;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import io.siddhi.core.util.error.handler.exception.ErrorStoreException;
import io.siddhi.core.util.error.handler.model.ErroneousEvent;
import io.siddhi.core.util.error.handler.model.ErrorEntry;
import io.siddhi.core.util.error.handler.model.PublishableErrorEntry;
import io.siddhi.core.util.error.handler.util.ErroneousEventType;
import io.siddhi.core.util.error.handler.util.ErrorHandlerUtils;
import io.siddhi.core.util.error.handler.util.ErrorOccurrence;
import io.siddhi.core.util.error.handler.util.ErrorType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;

/**
 * Denotes the abstract error store in which, error event entries will be stored.
 */
public abstract class ErrorStore {

    private static final Logger log = LogManager.getLogger(ErrorStore.class);

    private int bufferSize = 1024;
    private boolean dropWhenBufferFull = true;

    private Disruptor<PublishableErrorEntry> disruptor;
    private RingBuffer<PublishableErrorEntry> ringBuffer;

    public ErrorStore() {
        final ThreadFactory threadFactory = DaemonThreadFactory.INSTANCE;
        disruptor = new Disruptor<>(PublishableErrorEntry.EVENT_FACTORY, bufferSize, threadFactory, ProducerType.SINGLE,
                new BlockingWaitStrategy());
        disruptor.handleEventsWith(getEventHandler());
        ringBuffer = disruptor.start();
    }

    protected void produce(long timestamp, String siddhiAppName, String streamName,
                           byte[] eventAsBytes, String cause, byte[] stackTraceAsBytes,
                           byte[] originalPayloadAsBytes, String errorOccurrence, String eventType,
                           String errorType) {
        final long seq;
        try {
            seq = dropWhenBufferFull ? ringBuffer.tryNext() : ringBuffer.next();
            final PublishableErrorEntry publishableErrorEntry = ringBuffer.get(seq);
            publishableErrorEntry.setTimestamp(timestamp);
            publishableErrorEntry.setSiddhiAppName(siddhiAppName);
            publishableErrorEntry.setStreamName(streamName);
            publishableErrorEntry.setEventAsBytes(eventAsBytes);
            publishableErrorEntry.setCause(cause);
            publishableErrorEntry.setStackTraceAsBytes(stackTraceAsBytes);
            publishableErrorEntry.setOriginalPayloadAsBytes(originalPayloadAsBytes);
            publishableErrorEntry.setErrorOccurrence(errorOccurrence);
            publishableErrorEntry.setEventType(eventType);
            publishableErrorEntry.setErrorType(errorType);
            ringBuffer.publish(seq);
        } catch (InsufficientCapacityException e) {
            log.error("Insufficient capacity in the buffer.", e);
        }
    }

    protected EventHandler<PublishableErrorEntry>[] getEventHandler() {
        final EventHandler<PublishableErrorEntry> eventHandler = (event, sequence, endOfBatch) ->
                saveEntry(event.getTimestamp(), event.getSiddhiAppName(), event.getStreamName(),
                        event.getEventAsBytes(), event.getCause(), event.getStackTraceAsBytes(),
                        event.getOriginalPayloadAsBytes(), event.getErrorOccurrence(), event.getEventType(),
                        event.getErrorType());
        return new EventHandler[]{eventHandler};
    }

    public abstract void setProperties(Map properties);

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setDropWhenBufferFull(boolean dropWhenBufferFull) {
        this.dropWhenBufferFull = dropWhenBufferFull;
    }

    public void saveMappingError(String siddhiAppName, List<ErroneousEvent> erroneousEvents,
                                 String streamName) {
        for (ErroneousEvent erroneousEvent : erroneousEvents) {
            try {
                save(siddhiAppName, streamName, erroneousEvent, ErroneousEventType.PAYLOAD_STRING,
                        ErrorOccurrence.BEFORE_SOURCE_MAPPING, ErrorType.MAPPING);
            } catch (ErrorStoreException e) {
                log.error("Failed to save erroneous event.", e);
            }
        }
    }

    public void saveTransportError(String siddhiAppName, ErroneousEvent erroneousEvent, ErroneousEventType eventType,
                                   String streamName, ErrorOccurrence errorOccurrence) {
        try {
            save(siddhiAppName, streamName, erroneousEvent, eventType, errorOccurrence, ErrorType.TRANSPORT);
        } catch (ErrorStoreException e) {
            log.error("Failed to save erroneous event.", e);
        }
    }

    protected void save(String siddhiAppName, String streamName, ErroneousEvent erroneousEvent,
                        ErroneousEventType eventType, ErrorOccurrence errorOccurrence, ErrorType errorType)
            throws ErrorStoreException {
        long timestamp = System.currentTimeMillis();
        Object event = erroneousEvent.getEvent();
        Throwable throwable = erroneousEvent.getThrowable();
        String cause;
        if (throwable != null) {
            if (throwable.getCause() != null) {
                cause = throwable.getCause().getMessage();
            } else {
                cause = throwable.getMessage();
            }
        } else {
            cause = "Unknown";
        }

        try {
            Object originalPayload = erroneousEvent.getOriginalPayload();
            byte[] eventAsBytes = (event != null && eventType == ErroneousEventType.PAYLOAD_STRING) ?
                    ErrorHandlerUtils.getAsBytes(event.toString()) : ErrorHandlerUtils.getAsBytes(event);
            byte[] stackTraceAsBytes = (throwable != null) ?
                    ErrorHandlerUtils.getThrowableStackTraceAsBytes(throwable) : ErrorHandlerUtils.getAsBytes(null);
            byte[] originalPayloadAsBytes = ErrorHandlerUtils.getAsBytes(originalPayload);

            produce(timestamp, siddhiAppName, streamName, eventAsBytes, cause, stackTraceAsBytes,
                    originalPayloadAsBytes, errorOccurrence.toString(), eventType.toString(), errorType.toString());
        } catch (IOException e) {
            throw new ErrorStoreException("Failure occurred during byte array conversion.", e);
        }
    }

    protected abstract void saveEntry(long timestamp, String siddhiAppName, String streamName,
                                      byte[] eventAsBytes, String cause, byte[] stackTraceAsBytes,
                                      byte[] originalPayloadAsBytes, String errorOccurrence, String eventType,
                                      String errorType) throws ErrorStoreException;

    public abstract List<ErrorEntry> loadErrorEntries(String siddhiAppName, Map<String, String> queryParams);

    public abstract ErrorEntry loadErrorEntry(int id);

    protected ErrorEntry constructErrorEntry(int id, long timestamp, String siddhiAppName, String streamName,
                                             byte[] eventAsBytes, String cause, byte[] stackTraceAsBytes,
                                             byte[] originalPayloadAsBytes, ErrorOccurrence errorOccurrence,
                                             ErroneousEventType erroneousEventType, ErrorType errorType)
            throws IOException, ClassNotFoundException {
        String stackTrace = stackTraceAsBytes != null ? (String) ErrorHandlerUtils.getAsObject(stackTraceAsBytes) :
                null;
        String originalPayloadString = originalPayloadAsBytes != null ?
                ErrorHandlerUtils.getOriginalPayloadString(ErrorHandlerUtils.getAsObject(originalPayloadAsBytes)) :
                null;
        return new ErrorEntry(id, timestamp, siddhiAppName, streamName, eventAsBytes, cause, stackTrace,
                originalPayloadString, errorOccurrence, erroneousEventType, errorType);
    }

    public abstract void discardErrorEntry(int id);

    public abstract void discardErrorEntries(String siddhiAppName);

    public abstract int getTotalErrorEntriesCount();

    public abstract int getErrorEntriesCount(String siddhiAppName);

    public abstract void purge(Map retentionPolicyParams);
}
