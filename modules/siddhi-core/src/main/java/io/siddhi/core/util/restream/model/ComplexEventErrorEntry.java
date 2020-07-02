package io.siddhi.core.util.restream.model;

import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.util.restream.util.ErroneousEventType;
import io.siddhi.core.util.restream.util.ErrorOccurrence;
import io.siddhi.core.util.restream.util.ErrorType;

public class ComplexEventErrorEntry extends AbstractErrorEntry {
    private ComplexEvent event;

    public ComplexEventErrorEntry(int id, long timestamp, String siddhiAppName, String streamName, String cause, String stackTrace, String originalPayload, ErrorOccurrence errorOccurrence, ErrorType errorType, ComplexEvent event) {
        super(id, timestamp, siddhiAppName, streamName, cause, stackTrace, originalPayload, errorOccurrence, ErroneousEventType.COMPLEX_EVENT, errorType);
        this.event = event;
    }

    public ComplexEvent getEvent() {
        return event;
    }
}
