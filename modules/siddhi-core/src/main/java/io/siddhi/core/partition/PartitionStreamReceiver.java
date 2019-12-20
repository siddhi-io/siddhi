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
package io.siddhi.core.partition;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.Event;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.event.stream.converter.StreamEventConverter;
import io.siddhi.core.event.stream.converter.StreamEventConverterFactory;
import io.siddhi.core.partition.executor.PartitionExecutor;
import io.siddhi.core.query.QueryRuntime;
import io.siddhi.core.query.QueryRuntimeImpl;
import io.siddhi.core.query.input.stream.StreamRuntime;
import io.siddhi.core.stream.StreamJunction;
import io.siddhi.query.api.definition.StreamDefinition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Specific {@link StreamJunction.Receiver} implementation to pump events into partitions. This will send the event
 * to the matching partition.
 */
public class PartitionStreamReceiver implements StreamJunction.Receiver {

    private final StreamEventFactory streamEventFactory;
    private StreamEventConverter streamEventConverter;
    private String streamId;
    private MetaStreamEvent metaStreamEvent;
    private StreamDefinition streamDefinition;
    private SiddhiAppContext siddhiAppContext;
    private PartitionRuntimeImpl partitionRuntime;
    private List<PartitionExecutor> partitionExecutors;
    private Map<String, StreamJunction> streamJunctionMap = new HashMap<>();


    public PartitionStreamReceiver(SiddhiAppContext siddhiAppContext, MetaStreamEvent metaStreamEvent,
                                   StreamDefinition streamDefinition,
                                   List<PartitionExecutor> partitionExecutors,
                                   PartitionRuntime partitionRuntime) {
        this.metaStreamEvent = metaStreamEvent;
        this.streamDefinition = streamDefinition;
        this.partitionRuntime = (PartitionRuntimeImpl) partitionRuntime;
        this.partitionExecutors = partitionExecutors;
        this.siddhiAppContext = siddhiAppContext;
        this.streamId = streamDefinition.getId();
        this.streamEventFactory = new StreamEventFactory(metaStreamEvent);

    }

    public void init() {
        streamEventConverter = StreamEventConverterFactory.constructEventConverter(metaStreamEvent);
    }

    @Override
    public String getStreamId() {
        return streamId;
    }


    @Override
    public void receive(ComplexEvent complexEvent) {

        if (partitionExecutors.size() == 0) {
            ComplexEventChunk<ComplexEvent> outputEventChunk = new ComplexEventChunk<ComplexEvent>();
            ComplexEvent aComplexEvent = complexEvent;
            while (aComplexEvent != null) {
                StreamEvent newEvent = streamEventFactory.newInstance();
                streamEventConverter.convertComplexEvent(aComplexEvent, newEvent);
                outputEventChunk.add(newEvent);
                aComplexEvent = aComplexEvent.getNext();
            }
            send(outputEventChunk.getFirst());
        } else {
            if (complexEvent.getNext() == null) {
                for (PartitionExecutor partitionExecutor : partitionExecutors) {
                    StreamEvent newEvent = streamEventFactory.newInstance();
                    streamEventConverter.convertComplexEvent(complexEvent, newEvent);
                    String key = partitionExecutor.execute(newEvent);
                    send(key, newEvent);
                }
            } else {
                ComplexEventChunk<ComplexEvent> complexEventChunk = new ComplexEventChunk<ComplexEvent>();
                complexEventChunk.add(complexEvent);
                ComplexEventChunk<ComplexEvent> outputEventChunk = new ComplexEventChunk<ComplexEvent>();
                String currentKey = null;
                while (complexEventChunk.hasNext()) {
                    ComplexEvent aEvent = complexEventChunk.next();
                    complexEventChunk.remove();
                    StreamEvent newEvent = streamEventFactory.newInstance();
                    streamEventConverter.convertComplexEvent(aEvent, newEvent);
                    boolean currentEventMatchedPrevPartitionExecutor = false;
                    for (PartitionExecutor partitionExecutor : partitionExecutors) {
                        String key = partitionExecutor.execute(newEvent);
                        if (key != null) {
                            if (currentKey == null) {
                                currentKey = key;
                            } else if (!currentKey.equals(key)) {
                                if (!currentEventMatchedPrevPartitionExecutor) {
                                    ComplexEvent firstEvent = outputEventChunk.getFirst();
                                    send(currentKey, firstEvent);
                                    currentKey = key;
                                    outputEventChunk.clear();
                                } else {
                                    ComplexEvent firstEvent = outputEventChunk.getFirst();
                                    send(currentKey, firstEvent);
                                    currentKey = key;
                                    outputEventChunk.clear();
                                    StreamEvent cloneEvent = streamEventFactory.newInstance();
                                    streamEventConverter.convertComplexEvent(aEvent, cloneEvent);
                                    outputEventChunk.add(cloneEvent);
                                }
                            }
                            if (!currentEventMatchedPrevPartitionExecutor) {
                                outputEventChunk.add(newEvent);
                            }
                            currentEventMatchedPrevPartitionExecutor = true;
                        }
                    }
                }
                send(currentKey, outputEventChunk.getFirst());
                outputEventChunk.clear();
            }
        }

    }

    @Override
    public void receive(Event event) {
        StreamEvent newEvent = streamEventFactory.newInstance();
        streamEventConverter.convertEvent(event, newEvent);
        for (PartitionExecutor partitionExecutor : partitionExecutors) {
            String key = partitionExecutor.execute(newEvent);
            send(key, newEvent);
        }
        if (partitionExecutors.size() == 0) {
            send(newEvent);
        }
    }

    @Override
    public void receive(long timestamp, Object[] data) {
        StreamEvent newEvent = streamEventFactory.newInstance();
        streamEventConverter.convertData(timestamp, data, newEvent);
        if (partitionExecutors.size() == 0) {
            send(newEvent);
        } else {
            for (PartitionExecutor partitionExecutor : partitionExecutors) {
                String key = partitionExecutor.execute(newEvent);
                send(key, newEvent);
            }
        }
    }

    @Override
    public void receive(Event[] events) {
        if (partitionExecutors.size() == 0) {
            StreamEvent currentEvent;
            StreamEvent firstEvent = streamEventFactory.newInstance();
            streamEventConverter.convertEvent(events[0], firstEvent);
            currentEvent = firstEvent;
            for (int i = 1; i < events.length; i++) {
                StreamEvent nextEvent = streamEventFactory.newInstance();
                streamEventConverter.convertEvent(events[i], nextEvent);
                currentEvent.setNext(nextEvent);
                currentEvent = nextEvent;
            }
            send(firstEvent);
        } else {
            String key = null;
            StreamEvent firstEvent = null;
            StreamEvent currentEvent = null;
            for (Event event : events) {
                StreamEvent nextEvent = streamEventFactory.newInstance();
                streamEventConverter.convertEvent(event, nextEvent);
                for (PartitionExecutor partitionExecutor : partitionExecutors) {
                    String currentKey = partitionExecutor.execute(nextEvent);
                    if (currentKey != null) {
                        if (key == null) {
                            key = currentKey;
                            firstEvent = nextEvent;
                        } else if (!currentKey.equals(key)) {
                            send(key, firstEvent);
                            key = currentKey;
                            firstEvent = nextEvent;
                        } else {
                            currentEvent.setNext(nextEvent);
                        }
                        currentEvent = nextEvent;
                    }
                }
            }
            send(key, firstEvent);
        }

    }

    @Override
    public void receive(List<Event> events) {
        if (partitionExecutors.size() == 0) {
            StreamEvent firstEvent = null;
            StreamEvent currentEvent = null;
            for (Event event : events) {
                StreamEvent nextEvent = streamEventFactory.newInstance();
                streamEventConverter.convertEvent(event, nextEvent);
                if (firstEvent == null) {
                    firstEvent = nextEvent;
                } else {
                    currentEvent.setNext(nextEvent);
                }
                currentEvent = nextEvent;
            }
            send(firstEvent);
        } else {
            String key = null;
            StreamEvent firstEvent = null;
            StreamEvent currentEvent = null;
            for (Event event : events) {
                StreamEvent nextEvent = streamEventFactory.newInstance();
                streamEventConverter.convertEvent(event, nextEvent);
                for (PartitionExecutor partitionExecutor : partitionExecutors) {
                    String currentKey = partitionExecutor.execute(nextEvent);
                    if (currentKey != null) {
                        if (key == null) {
                            key = currentKey;
                            firstEvent = nextEvent;
                        } else if (!currentKey.equals(key)) {
                            send(key, firstEvent);
                            key = currentKey;
                            firstEvent = nextEvent;
                        } else {
                            currentEvent.setNext(nextEvent);
                        }
                        currentEvent = nextEvent;
                    }
                }
            }
            send(key, firstEvent);
        }
    }

    private void send(String key, ComplexEvent event) {
        if (key != null) {
            SiddhiAppContext.startPartitionFlow(key);
            try {
                partitionRuntime.initPartition();
                streamJunctionMap.get(streamId).sendEvent(event);
            } finally {
                SiddhiAppContext.stopPartitionFlow();
            }
        }
    }

    private void send(ComplexEvent event) {
        for (String key : partitionRuntime.getPartitionKeys()) {
            SiddhiAppContext.startPartitionFlow(key);
            try {
                streamJunctionMap.get(streamId).sendEvent(event);
            } finally {
                SiddhiAppContext.stopPartitionFlow();
            }
        }
    }

    /**
     * create local streamJunctions through which events received by partitionStreamReceiver, are sent to
     * queryStreamReceivers
     *
     * @param queryRuntimeList queryRuntime list of the partition
     */
    public void addStreamJunction(List<QueryRuntime> queryRuntimeList) {
        StreamJunction streamJunction = streamJunctionMap.get(streamId);
        if (streamJunction == null) {
            streamJunction = partitionRuntime.getInnerPartitionStreamReceiverStreamJunctionMap().get(streamId);
            if (streamJunction == null) {
                streamJunction = createStreamJunction();
                partitionRuntime.addInnerpartitionStreamReceiverStreamJunction(streamId, streamJunction);
            }
            streamJunctionMap.put(streamId, streamJunction);
        }
        for (QueryRuntime queryRuntime : queryRuntimeList) {
            StreamRuntime streamRuntime = ((QueryRuntimeImpl) queryRuntime).getStreamRuntime();
            for (int i = 0; i < ((QueryRuntimeImpl) queryRuntime).getInputStreamId().size(); i++) {
                if ((streamRuntime.getSingleStreamRuntimes().get(i)).
                        getProcessStreamReceiver().getStreamId().equals(streamId)) {
                    streamJunction.subscribe((streamRuntime.getSingleStreamRuntimes().get(i))
                            .getProcessStreamReceiver());
                }
            }
        }
    }

    private StreamJunction createStreamJunction() {
        return new StreamJunction(streamDefinition, siddhiAppContext.getExecutorService(),
                siddhiAppContext.getBufferSize(), null, siddhiAppContext);
    }

}
