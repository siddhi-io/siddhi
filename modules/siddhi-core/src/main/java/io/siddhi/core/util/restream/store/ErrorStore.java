package io.siddhi.core.util.restream.store;

import io.siddhi.core.util.restream.model.AbstractErrorEntry;
import io.siddhi.core.util.restream.model.ErroneousEvent;
import io.siddhi.core.util.restream.util.ErroneousEventType;

import java.util.List;
import java.util.Map;

public interface ErrorStore {
    void setProperties(Map properties);

    void saveBeforeSourceMappingError(String siddhiAppName, List<ErroneousEvent> erroneousEvents, String streamName);

    void saveOnSinkError(String siddhiAppName, ErroneousEvent erroneousEvent, ErroneousEventType eventType,
                         String streamName);

    void saveOnStreamError(String siddhiAppName, ErroneousEvent erroneousEvent, ErroneousEventType eventType,
                           String streamName);

    List<AbstractErrorEntry> loadErrorEntries(String siddhiAppName, Map<String, String> queryParams);

    void discardErroneousEvent(int id);
}
