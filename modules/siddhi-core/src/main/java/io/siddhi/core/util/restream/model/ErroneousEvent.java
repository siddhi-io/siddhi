package io.siddhi.core.util.restream.model;

import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.Event;
import io.siddhi.core.util.restream.util.ErroneousEventType;
import io.siddhi.core.util.restream.util.ErrorOccurrence;
import io.siddhi.core.util.restream.util.ErrorType;

public class ErroneousEvent extends AbstractErroneousEvent {
    private Event event;

    public ErroneousEvent(int id, long timestamp, String siddhiAppName, String streamName, ErrorOccurrence errorOccurrence, ErrorType errorType, String errorMessage, Event event) {
        super(id, timestamp, siddhiAppName, streamName, errorOccurrence, errorType, ErroneousEventType.EVENT, errorMessage);
        this.event = event;
    }

    public Event getEvent() {
        return event;
    }
}
