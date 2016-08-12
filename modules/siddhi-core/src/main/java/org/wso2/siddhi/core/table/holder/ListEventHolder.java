package org.wso2.siddhi.core.table.holder;

import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventPool;
import org.wso2.siddhi.core.event.stream.converter.StreamEventConverter;

import java.util.LinkedList;

/**
 * Created by suho on 5/22/16.
 */
public class ListEventHolder extends LinkedList<StreamEvent> implements EventHolder {

    private StreamEventPool tableStreamEventPool;
    private StreamEventConverter eventConverter;

    public ListEventHolder(StreamEventPool tableStreamEventPool, StreamEventConverter eventConverter) {
        this.tableStreamEventPool = tableStreamEventPool;
        this.eventConverter = eventConverter;
    }

    @Override
    public void add(ComplexEventChunk<StreamEvent> addingEventChunk) {
        addingEventChunk.reset();
        while (addingEventChunk.hasNext()) {
            ComplexEvent complexEvent = addingEventChunk.next();
            StreamEvent streamEvent = tableStreamEventPool.borrowEvent();
            eventConverter.convertComplexEvent(complexEvent, streamEvent);
            this.add(streamEvent);
        }
    }
}
