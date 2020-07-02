package io.siddhi.core.util.restream.model;

import io.siddhi.core.event.Event;
import io.siddhi.core.util.restream.util.ErroneousEventType;
import io.siddhi.core.util.restream.util.ErrorOccurrence;
import io.siddhi.core.util.restream.util.ErrorType;

import java.util.List;

public class EventListErrorEntry extends AbstractErrorEntry {
    private List<Event> event;

    public EventListErrorEntry(int id, long timestamp, String siddhiAppName, String streamName, String cause, String stackTrace, String originalPayload, ErrorOccurrence errorOccurrence, ErrorType errorType, List<Event> event) {
        super(id, timestamp, siddhiAppName, streamName, cause, stackTrace, originalPayload, errorOccurrence, ErroneousEventType.EVENT_LIST, errorType);
        this.event = event;
    }

    public List<Event> getEvent() {
        return event;
    }
}
