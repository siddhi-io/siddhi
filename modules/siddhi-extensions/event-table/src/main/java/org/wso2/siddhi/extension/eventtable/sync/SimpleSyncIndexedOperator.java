/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.extension.eventtable.sync;

import org.apache.hadoop.util.bloom.Key;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.StreamEventPool;
import org.wso2.siddhi.core.event.stream.converter.ZeroStreamEventConverter;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.util.collection.operator.Finder;
import org.wso2.siddhi.core.util.collection.operator.Operator;

import java.util.Map;

import static org.wso2.siddhi.core.util.SiddhiConstants.ANY;

/**
 * Operator which is related to Indexed In-memory table operations.
 */
public class SimpleSyncIndexedOperator implements Operator {
    private final long withinTime;
    private final StreamEventPool streamEventPool;
    private final ZeroStreamEventConverter streamEventConverter;
    private int outputAttributeSize;
    private ExpressionExecutor expressionExecutor;
    private int matchingEventPosition;
    private int indexPosition;
    private SyncTableHandler syncTableHandler;
    private boolean isBloomEnabled;


    public SimpleSyncIndexedOperator(ExpressionExecutor expressionExecutor, int matchingEventPosition, long withinTime,
                                     int matchingStreamOutputAttributeSize, int indexPosition, SyncTableHandler syncTableHandler) {
        this.expressionExecutor = expressionExecutor;
        this.matchingEventPosition = matchingEventPosition;
        this.withinTime = withinTime;
        this.outputAttributeSize = matchingStreamOutputAttributeSize;
        this.streamEventPool = new StreamEventPool(0, 0, matchingStreamOutputAttributeSize, 10);
        this.streamEventConverter = new ZeroStreamEventConverter();
        this.indexPosition = indexPosition;
        this.syncTableHandler = syncTableHandler;

        if (syncTableHandler.isBloomFilterEnabled()) {
            this.isBloomEnabled = true;
        }

    }

    private boolean outsideTimeWindow(ComplexEvent matchingEvent, StreamEvent streamEvent) {
        if (withinTime != ANY) {
            long timeDifference = matchingEvent.getTimestamp() - streamEvent.getTimestamp();
            if ((0 > timeDifference) || (timeDifference > withinTime)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Finder cloneFinder() {
        return new SimpleSyncIndexedOperator(expressionExecutor, matchingEventPosition, withinTime, outputAttributeSize,
                indexPosition, syncTableHandler);
    }

    @Override
    public StreamEvent find(ComplexEvent matchingEvent, Object candidateEvents, StreamEventCloner streamEventCloner) {
        Object matchingKey = expressionExecutor.execute(matchingEvent);
        if (isBloomEnabled) {
            boolean mightContain = syncTableHandler.getBloomFilters()[0].membershipTest(new Key(matchingKey.toString().getBytes()));
            if (!mightContain) {
                return null;
            }
        }

        if (candidateEvents instanceof Map) {
            StreamEvent streamEvent = ((Map<Object, StreamEvent>) candidateEvents).get(matchingKey);
            if (streamEvent == null) {
                return null;
            } else {
                if (outsideTimeWindow(matchingEvent, streamEvent)) {
                    return null;
                }
                return streamEventCloner.copyStreamEvent(streamEvent);
            }
        } else {
            throw new OperationNotSupportedException(SimpleSyncIndexedOperator.class.getCanonicalName() +
                    " does not support " + candidateEvents.getClass().getCanonicalName());
        }
    }

    @Override
    public void delete(ComplexEventChunk deletingEventChunk, Object candidateEvents) {
        deletingEventChunk.reset();
        while (deletingEventChunk.hasNext()) {
            ComplexEvent deletingEvent = deletingEventChunk.next();
            Object matchingKey = expressionExecutor.execute(deletingEvent);
            if (candidateEvents instanceof Map) {
                StreamEvent streamEvent = ((Map<Object, StreamEvent>) candidateEvents).get(matchingKey);
                if (streamEvent != null) {
                    if (outsideTimeWindow(deletingEvent, streamEvent)) {
                        return;
                    }
                    ((Map<Object, StreamEvent>) candidateEvents).remove(matchingKey);
                }
            } else {
                throw new OperationNotSupportedException(SimpleSyncIndexedOperator.class.getCanonicalName() +
                        " does not support " + candidateEvents.getClass().getCanonicalName());
            }
        }
    }

    @Override
    public void update(ComplexEventChunk updatingEventChunk, Object candidateEvents, int[] mappingPosition) {
        updatingEventChunk.reset();
        while (updatingEventChunk.hasNext()) {
            ComplexEvent updatingEvent = updatingEventChunk.next();
            Object matchingKey = expressionExecutor.execute(updatingEvent);
            if (candidateEvents instanceof Map) {
                StreamEvent streamEvent = ((Map<Object, StreamEvent>) candidateEvents).get(matchingKey);
                if (streamEvent != null) {
                    if (outsideTimeWindow(updatingEvent, streamEvent)) {
                        return;
                    }
                    for (int i = 0, size = mappingPosition.length; i < size; i++) {
                        streamEvent.setOutputData(updatingEvent.getOutputData()[i], mappingPosition[i]);
                    }
                }
            } else {
                throw new OperationNotSupportedException(SimpleSyncIndexedOperator.class.getCanonicalName() +
                        " does not support " + candidateEvents.getClass().getCanonicalName());
            }
        }
    }

    @Override
    public void overwriteOrAdd(ComplexEventChunk overwritingOrAddingEventChunk, Object candidateEvents,
                               int[] mappingPosition) {
        overwritingOrAddingEventChunk.reset();
        while (overwritingOrAddingEventChunk.hasNext()) {
            if (candidateEvents instanceof Map) {
                ComplexEvent complexEvent = overwritingOrAddingEventChunk.next();
                StreamEvent streamEvent = streamEventPool.borrowEvent();
                streamEventConverter.convertStreamEvent(complexEvent, streamEvent);
                ((Map<Object, StreamEvent>) candidateEvents).put(streamEvent.getOutputData()[indexPosition],
                        streamEvent);
            } else {
                throw new OperationNotSupportedException(SimpleSyncIndexedOperator.class.getCanonicalName() +
                        " does not support " + candidateEvents.getClass().getCanonicalName());
            }
        }
    }

    @Override
    public boolean contains(ComplexEvent matchingEvent, Object candidateEvents) {
        StreamEvent matchingStreamEvent;
        if (matchingEvent instanceof StreamEvent) {
            matchingStreamEvent = ((StreamEvent) matchingEvent);
        } else {
            matchingStreamEvent = ((StateEvent) matchingEvent).getStreamEvent(matchingEventPosition);
        }
        Object matchingKey = expressionExecutor.execute(matchingStreamEvent);

        if (isBloomEnabled) {
            boolean mightContain = syncTableHandler.getBloomFilters()[0].membershipTest(new Key(matchingKey.toString().getBytes()));
            if (!mightContain) {
                return false;
            }
        }

        if (candidateEvents instanceof Map) {
            StreamEvent streamEvent = ((Map<Object, StreamEvent>) candidateEvents).get(matchingKey);
            return streamEvent != null && !outsideTimeWindow(matchingStreamEvent, streamEvent);
        } else {
            throw new OperationNotSupportedException(SimpleSyncIndexedOperator.class.getCanonicalName() +
                    " does not support " + candidateEvents.getClass().getCanonicalName());
        }
    }
}
