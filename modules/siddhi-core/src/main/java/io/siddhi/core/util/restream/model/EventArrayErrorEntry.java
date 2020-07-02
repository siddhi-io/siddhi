package io.siddhi.core.util.restream.model;

import io.siddhi.core.event.Event;
import io.siddhi.core.util.restream.util.ErroneousEventType;
import io.siddhi.core.util.restream.util.ErrorOccurrence;
import io.siddhi.core.util.restream.util.ErrorType;

public class EventArrayErrorEntry extends AbstractErrorEntry {
    private Event[] event;

    public EventArrayErrorEntry(int id, long timestamp, String siddhiAppName, String streamName, String cause, String stackTrace, String originalPayload, ErrorOccurrence errorOccurrence, ErrorType errorType, Event[] event) {
        super(id, timestamp, siddhiAppName, streamName, cause, stackTrace, originalPayload, errorOccurrence, ErroneousEventType.EVENT_ARRAY, errorType);
        this.event = event;
    }

    public Event[] getEvent() {
        return event;
    }
}
