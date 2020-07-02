package io.siddhi.core.util.restream.model;

import io.siddhi.core.util.restream.util.ErroneousEventType;
import io.siddhi.core.util.restream.util.ErrorOccurrence;
import io.siddhi.core.util.restream.util.ErrorType;

public class ErroneousPayloadStringEvent extends AbstractErroneousEvent {
    private String event;

    public ErroneousPayloadStringEvent(int id, long timestamp, String siddhiAppName, String streamName, ErrorOccurrence errorOccurrence, ErrorType errorType, String errorMessage, String event) {
        super(id, timestamp, siddhiAppName, streamName, errorOccurrence, errorType, ErroneousEventType.PAYLOAD_STRING, errorMessage);
        this.event = event;
    }

    public String getEvent() {
        return event;
    }
}
