/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.core.table.holder;

import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.event.stream.converter.StreamEventConverter;
import io.siddhi.core.event.stream.holder.SnapshotableStreamEventQueue;
import io.siddhi.core.event.stream.holder.StreamEventClonerHolder;

/**
 * Holder object to contain a list of {@link StreamEvent}. Users can add {@link ComplexEventChunk}s to the
 * {@link ListEventHolder} where events in chunk will be added to the {@link StreamEvent} list.
 */
public class ListEventHolder extends SnapshotableStreamEventQueue implements EventHolder {

    private static final long serialVersionUID = 4695745058501269511L;
    private StreamEventFactory tableStreamEventFactory;
    private StreamEventConverter eventConverter;

    public ListEventHolder(StreamEventFactory tableStreamEventFactory, StreamEventConverter eventConverter,
                           StreamEventClonerHolder streamEventClonerHolder) {
        super(streamEventClonerHolder);
        this.tableStreamEventFactory = tableStreamEventFactory;
        this.eventConverter = eventConverter;
    }

    @Override
    public void add(ComplexEventChunk<StreamEvent> addingEventChunk) {
        addingEventChunk.reset();
        while (addingEventChunk.hasNext()) {
            size++;
            ComplexEvent complexEvent = addingEventChunk.next();
            StreamEvent streamEvent = tableStreamEventFactory.newInstance();
            eventConverter.convertComplexEvent(complexEvent, streamEvent);
            this.add(streamEvent);
        }
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public void deleteAll() {
        super.clear();
        size = 0;
    }
}
