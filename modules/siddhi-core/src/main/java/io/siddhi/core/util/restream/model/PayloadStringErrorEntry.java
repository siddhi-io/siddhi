package io.siddhi.core.util.restream.model;

import io.siddhi.core.util.restream.util.ErroneousEventType;
import io.siddhi.core.util.restream.util.ErrorOccurrence;
import io.siddhi.core.util.restream.util.ErrorType;

public class PayloadStringErrorEntry extends AbstractErrorEntry {
    private String event;

    public PayloadStringErrorEntry(int id, long timestamp, String siddhiAppName, String streamName, String cause, String stackTrace, String originalPayload, ErrorOccurrence errorOccurrence, ErrorType errorType, String event) {
        super(id, timestamp, siddhiAppName, streamName, cause, stackTrace, originalPayload, errorOccurrence, ErroneousEventType.PAYLOAD_STRING, errorType);
        this.event = event;
    }

    public String getEvent() {
        return event;
    }
}
