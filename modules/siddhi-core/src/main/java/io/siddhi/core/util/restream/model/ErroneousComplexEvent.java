package io.siddhi.core.util.restream.model;

import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.Event;
import io.siddhi.core.util.restream.util.ErroneousEventType;
import io.siddhi.core.util.restream.util.ErrorOccurrence;
import io.siddhi.core.util.restream.util.ErrorType;

public class ErroneousComplexEvent extends AbstractErroneousEvent {
    private ComplexEvent event;

    public ErroneousComplexEvent(int id, long timestamp, String siddhiAppName, String streamName, ErrorOccurrence errorOccurrence, ErrorType errorType, String errorMessage, ComplexEvent event) {
        super(id, timestamp, siddhiAppName, streamName, errorOccurrence, errorType, ErroneousEventType.COMPLEX_EVENT, errorMessage);
        this.event = event;
    }

    public ComplexEvent getEvent() {
        return event;
    }
}
