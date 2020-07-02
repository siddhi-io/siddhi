package io.siddhi.core.util.restream.util;

import io.siddhi.core.util.restream.store.ErrorStore;

import java.util.List;

public class ErrorStoreHelper {
    public static void storeFailedEvents(ErrorStore errorStore, ErrorOccurrence occurrence, String siddhiAppName,
                                         Object failedEvents, String streamName, Exception e) {
        // TODO failed event (singular)! not events (in param)
        if (errorStore != null) {
            if (occurrence == ErrorOccurrence.BEFORE_SOURCE_MAPPING && failedEvents instanceof List) {
                errorStore.saveBeforeSourceMappingError(siddhiAppName, (List<Object>) failedEvents, streamName, e);
            } else if (occurrence == ErrorOccurrence.STORE_ON_SINK_ERROR) {
                errorStore.saveOnSinkError(siddhiAppName, failedEvents,
                        ErrorStoreUtils.getErroneousEventType(failedEvents), streamName, e);
            } else if (occurrence == ErrorOccurrence.STORE_ON_STREAM_ERROR) {
                errorStore.saveOnStreamError(siddhiAppName, failedEvents,
                        ErrorStoreUtils.getErroneousEventType(failedEvents), streamName, e);
            }
        }
    }
}
