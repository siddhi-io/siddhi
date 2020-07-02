package io.siddhi.core.util.restream.model;

import io.siddhi.core.util.restream.util.ErroneousEventType;
import io.siddhi.core.util.restream.util.ErrorOccurrence;
import io.siddhi.core.util.restream.util.ErrorType;

public abstract class AbstractErroneousEvent { // TODO name something like storedRecord instead of 'event' here
    private int id;
    private long timestamp;
    private String siddhiAppName;
    private String streamName;
    private ErrorOccurrence errorOccurrence;
    private ErroneousEventType eventType;
    private ErrorType errorType;
    private String errorMessage;

    public AbstractErroneousEvent(int id, long timestamp, String siddhiAppName, String streamName,
                                  ErrorOccurrence errorOccurrence, ErrorType errorType, ErroneousEventType eventType,
                                  String errorMessage) {
        this.id = id;
        this.timestamp = timestamp;
        this.siddhiAppName = siddhiAppName;
        this.streamName = streamName;
        this.errorOccurrence = errorOccurrence;
        this.errorType = errorType;
        this.eventType = eventType;
        this.errorMessage = errorMessage;
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

    public ErrorOccurrence getErrorOccurrence() {
        return errorOccurrence;
    }

    public ErrorType getErrorType() {
        return errorType;
    }

    public ErroneousEventType getEventType() {
        return eventType;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
