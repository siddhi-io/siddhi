package io.siddhi.core.util.restream.util;

import io.siddhi.core.util.restream.model.ErroneousEvent;
import io.siddhi.core.util.restream.store.ErrorStore;

import java.util.List;

public class ErrorStoreHelper {
    public static void storeErroneousEvent(ErrorStore errorStore, ErrorOccurrence occurrence, String siddhiAppName,
                                           Object erroneousEvent, String streamName) {
        if (errorStore != null) {
            if (occurrence == ErrorOccurrence.BEFORE_SOURCE_MAPPING && erroneousEvent instanceof List) {
                errorStore.saveBeforeSourceMappingError(siddhiAppName, (List<ErroneousEvent>) erroneousEvent,
                        streamName);
            } else if (occurrence == ErrorOccurrence.STORE_ON_SINK_ERROR) {
                errorStore.saveOnSinkError(siddhiAppName, (ErroneousEvent) erroneousEvent,
                        ErrorStoreUtils.getErroneousEventType(((ErroneousEvent) erroneousEvent).getEvent()),
                        streamName);
            } else if (occurrence == ErrorOccurrence.STORE_ON_STREAM_ERROR) {
                errorStore.saveOnStreamError(siddhiAppName, (ErroneousEvent) erroneousEvent,
                        ErrorStoreUtils.getErroneousEventType(((ErroneousEvent) erroneousEvent).getEvent()),
                        streamName);
            }
        }
    }
}
