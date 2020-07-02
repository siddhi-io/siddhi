package io.siddhi.core.util.restream.model;

import io.siddhi.core.event.Event;
import io.siddhi.core.util.restream.util.ErroneousEventType;
import io.siddhi.core.util.restream.util.ErrorOccurrence;
import io.siddhi.core.util.restream.util.ErrorType;

public class EventErrorEntry extends AbstractErrorEntry {
    private Event event;

    public EventErrorEntry(int id, long timestamp, String siddhiAppName, String streamName, String cause, String stackTrace, String originalPayload, ErrorOccurrence errorOccurrence, ErrorType errorType, Event event) {
        super(id, timestamp, siddhiAppName, streamName, cause, stackTrace, originalPayload, errorOccurrence, ErroneousEventType.EVENT, errorType);
        this.event = event;
    }

    public Event getEvent() {
        return event;
    }
}
