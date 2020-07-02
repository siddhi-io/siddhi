package io.siddhi.core.util.restream.store;

import io.siddhi.core.util.restream.model.AbstractErroneousEvent;
import io.siddhi.core.util.restream.util.ErroneousEventType;

import java.util.List;
import java.util.Map;

public interface ErrorStore {
    void setProperties(Map properties);

    void saveBeforeSourceMappingError(String siddhiAppName, List<Object> failedEvents, String streamName, Exception e);

    void saveOnSinkError(String siddhiAppName, Object failedEvent, ErroneousEventType eventType,
                         String streamName, Exception e);

    void saveOnStreamError(String siddhiAppName, Object failedEvent, ErroneousEventType eventType,
                           String streamName, Exception e);

    List<AbstractErroneousEvent> loadErroneousEvents(String siddhiAppName, Map<String, String> queryParams);

    void discardErroneousEvent(int id);
}
