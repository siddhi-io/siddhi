package io.siddhi.core.table.record;

import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.state.StateEventFactory;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.query.api.definition.Attribute;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Class used to reduce the fail to update events to such that they can be inserted at once.
 */
public class UpdateOrInsertReducer implements Serializable {

    private final StreamEventFactory streamEventFactory;
    private final StateEventFactory stateEventFactory;
    private final int storeEventIndex;
    private final int streamEventIndex;
    private final Map<String, Integer> attributeMap;
    private ExpressionExecutor inMemoryCompiledCondition;

    public UpdateOrInsertReducer(ExpressionExecutor inMemoryCompiledCondition,
                                 MatchingMetaInfoHolder matchingMetaInfoHolder) {

        this.inMemoryCompiledCondition = inMemoryCompiledCondition;
        this.streamEventFactory = new StreamEventFactory(
                matchingMetaInfoHolder.getMetaStateEvent().getMetaStreamEvent(
                        matchingMetaInfoHolder.getStoreEventIndex()));
        this.stateEventFactory = new StateEventFactory(
                matchingMetaInfoHolder.getMetaStateEvent());
        storeEventIndex = matchingMetaInfoHolder.getStoreEventIndex();
        streamEventIndex = matchingMetaInfoHolder.getMatchingStreamEventIndex();
        attributeMap = new HashMap<>();
        List<Attribute> attributeList = matchingMetaInfoHolder.getStoreDefinition().getAttributeList();
        for (int i = 0; i < attributeList.size(); i++) {
            Attribute attribute = attributeList.get(i);
            attributeMap.put(attribute.getName(), i);
        }
    }

    public List<Object[]> reduceEventsForInsert(List<Object[]> failedRecords,
                                                Map<String, ExpressionExecutor> inMemorySetExecutors) {
        ComplexEventChunk<StreamEvent> toInsertEventChunk = new ComplexEventChunk<>();
        StateEvent joinEvent = stateEventFactory.newInstance();
        for (Object[] data : failedRecords) {
            StreamEvent failedEvent = streamEventFactory.newInstance();
            failedEvent.setOutputData(data);
            joinEvent.setEvent(streamEventIndex, failedEvent);
            boolean updated = false;
            toInsertEventChunk.reset();
            while (toInsertEventChunk.hasNext()) {
                StreamEvent toInsertEvent = toInsertEventChunk.next();
                joinEvent.setEvent(storeEventIndex, toInsertEvent);
                if ((Boolean) inMemoryCompiledCondition.execute(joinEvent)) {
                    for (Map.Entry<String, ExpressionExecutor> entry :
                            inMemorySetExecutors.entrySet()) {
                        toInsertEvent.setOutputData(entry.getValue().execute(failedEvent),
                                attributeMap.get(entry.getKey()));
                    }
                    updated = true;
                }
            }
            if (!updated) {
                toInsertEventChunk.add(failedEvent);
            }
        }
        List<Object[]> toInsertRecords = new LinkedList<>();
        toInsertEventChunk.reset();
        while (toInsertEventChunk.hasNext()) {
            StreamEvent streamEvent = toInsertEventChunk.next();
            toInsertRecords.add(streamEvent.getOutputData());
        }
        return toInsertRecords;
    }


}
