package io.siddhi.core.util.restream.model;

import io.siddhi.core.util.restream.util.ErroneousEventType;
import io.siddhi.core.util.restream.util.ErrorOccurrence;
import io.siddhi.core.util.restream.util.ErrorType;

public abstract class AbstractErrorEntry {
    private int id;
    private long timestamp;
    private String siddhiAppName;
    private String streamName;
    private String cause;
    private String stackTrace;
    private String originalPayload;
    private ErrorOccurrence errorOccurrence;
    private ErroneousEventType eventType;
    private ErrorType errorType;

    public AbstractErrorEntry(int id, long timestamp, String siddhiAppName, String streamName, String cause, String stackTrace, String originalPayload, ErrorOccurrence errorOccurrence, ErroneousEventType eventType, ErrorType errorType) {
        this.id = id;
        this.timestamp = timestamp;
        this.siddhiAppName = siddhiAppName;
        this.streamName = streamName;
        this.cause = cause;
        this.stackTrace = stackTrace;
        this.originalPayload = originalPayload;
        this.errorOccurrence = errorOccurrence;
        this.eventType = eventType;
        this.errorType = errorType;
    }

    public int getId() {
        return id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getSiddhiAppName() {
        return siddhiAppName;
    }

    public String getStreamName() {
        return streamName;
    }

    public String getCause() {
        return cause;
    }

    public String getStackTrace() {
        return stackTrace;
    }

    public String getOriginalPayload() {
        return originalPayload;
    }

    public ErrorOccurrence getErrorOccurrence() {
        return errorOccurrence;
    }

    public ErroneousEventType getEventType() {
        return eventType;
    }

    public ErrorType getErrorType() {
        return errorType;
    }
}
