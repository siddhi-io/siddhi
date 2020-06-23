package io.siddhi.core.util.preservation;

import io.siddhi.core.exception.MappingFailedException;

import java.util.List;

public interface PreservationStore {
    void saveTransportError(String siddhiAppName, String streamName, List<Object> failedEvents, Exception e);

    void saveMappingError(String siddhiAppName, String streamName, List<Object> failedEvents, MappingFailedException e);
}
