package io.siddhi.core.util.restream.model;

import io.siddhi.core.event.Event;
import io.siddhi.core.util.restream.util.ErroneousEventType;
import io.siddhi.core.util.restream.util.ErrorOccurrence;
import io.siddhi.core.util.restream.util.ErrorType;

import java.util.List;

public class ErroneousEventListEvent extends AbstractErroneousEvent {
    private List<Event> event;

    public ErroneousEventListEvent(int id, long timestamp, String siddhiAppName, String streamName, ErrorOccurrence errorOccurrence, ErrorType errorType, String errorMessage, List<Event> event) {
        super(id, timestamp, siddhiAppName, streamName, errorOccurrence, errorType, ErroneousEventType.EVENT_LIST, errorMessage);
        this.event = event;
    }

    public List<Event> getEvent() {
        return event;
    }
}
