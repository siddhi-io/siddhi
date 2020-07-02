package io.siddhi.core.util.restream.model;

import io.siddhi.core.event.Event;
import io.siddhi.core.util.restream.util.ErroneousEventType;
import io.siddhi.core.util.restream.util.ErrorOccurrence;
import io.siddhi.core.util.restream.util.ErrorType;

import java.util.List;

public class ErroneousEventArrayEvent extends AbstractErroneousEvent {
    private Event[] event;

    public ErroneousEventArrayEvent(int id, long timestamp, String siddhiAppName, String streamName, ErrorOccurrence errorOccurrence, ErrorType errorType, String errorMessage, Event[] event) {
        super(id, timestamp, siddhiAppName, streamName, errorOccurrence, errorType, ErroneousEventType.EVENT_ARRAY, errorMessage);
        this.event = event;
    }

    public Event[] getEvent() {
        return event;
    }
}
