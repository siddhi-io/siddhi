package io.siddhi.core.util.restream.util;

import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.Event;

import java.util.List;

public class ErrorStoreUtils {
    public static ErroneousEventType getErroneousEventType(Object event) {
        if (event instanceof ComplexEvent) {
            return ErroneousEventType.COMPLEX_EVENT;
        } else if (event instanceof Event) {
            return ErroneousEventType.EVENT;
        } else if (event instanceof Event[]) {
            return ErroneousEventType.EVENT_ARRAY;
        } else if (event instanceof List) {
            return ErroneousEventType.EVENT_LIST;
        } else if (event instanceof String) {
            return ErroneousEventType.PAYLOAD_STRING;
        }
        return null;
    }
}
