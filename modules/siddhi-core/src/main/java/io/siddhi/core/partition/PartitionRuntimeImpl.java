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
import io.siddhi.core.event.state.MetaStateEvent;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.partition.executor.PartitionExecutor;
import io.siddhi.core.query.QueryRuntime;
import io.siddhi.core.query.QueryRuntimeImpl;
import io.siddhi.core.query.input.stream.join.JoinStreamRuntime;
import io.siddhi.core.query.input.stream.single.SingleStreamRuntime;
import io.siddhi.core.query.input.stream.state.StateStreamRuntime;
import io.siddhi.core.query.output.callback.InsertIntoStreamCallback;
import io.siddhi.core.query.output.callback.InsertIntoWindowCallback;
import io.siddhi.core.stream.StreamJunction;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.parser.helper.DefinitionParserHelper;
import io.siddhi.core.util.parser.helper.QueryParserHelper;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateHolder;
import io.siddhi.core.util.statistics.MemoryUsageTracker;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.annotation.Element;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.exception.DuplicateAnnotationException;
import io.siddhi.query.api.execution.partition.Partition;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.state.CountStateElement;
import io.siddhi.query.api.execution.query.input.state.EveryStateElement;
import io.siddhi.query.api.execution.query.input.state.LogicalStateElement;
import io.siddhi.query.api.execution.query.input.state.NextStateElement;
import io.siddhi.query.api.execution.query.input.state.StateElement;
import io.siddhi.query.api.execution.query.input.state.StreamStateElement;
import io.siddhi.query.api.execution.query.input.stream.JoinInputStream;
import io.siddhi.query.api.execution.query.input.stream.SingleInputStream;
import io.siddhi.query.api.execution.query.input.stream.StateInputStream;
import io.siddhi.query.api.execution.query.output.stream.InsertIntoStream;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.util.AnnotationHelper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Runtime class to handle partitioning. It will hold all information regarding current partitions and wil create
 * partition dynamically during runtime.
 */
public class PartitionRuntimeImpl implements PartitionRuntime {

    private final StateHolder<PartitionState> stateHolder;
    //default every 5 min
    private long purgeExecutionInterval = 300000;
    private boolean purgingEnabled = false;
    private long purgeIdlePeriod = 0;
    private String partitionName;
    private Partition partition;
    private ConcurrentMap<String, StreamJunction> localStreamJunctionMap = new ConcurrentHashMap<>();
    private ConcurrentMap<String, StreamJunction> innerPartitionStreamReceiverStreamJunctionMap =
            new ConcurrentHashMap<>();    //contains definition
    private ConcurrentMap<String, AbstractDefinition> localStreamDefinitionMap =
            new ConcurrentHashMap<>(); //contains stream definition
    private ConcurrentMap<String, AbstractDefinition> streamDefinitionMap;
    private ConcurrentMap<String, AbstractDefinition> windowDefinitionMap;
    private ConcurrentMap<String, StreamJunction> streamJunctionMap;
    private List<QueryRuntime> queryRuntimeList = new ArrayList<QueryRuntime>();
    private ConcurrentMap<String, PartitionStreamReceiver> partitionStreamReceivers = new ConcurrentHashMap<>();
    private SiddhiAppContext siddhiAppContext;

    public PartitionRuntimeImpl(ConcurrentMap<String, AbstractDefinition> streamDefinitionMap,
                                ConcurrentMap<String, AbstractDefinition> windowDefinitionMap,
                                ConcurrentMap<String, StreamJunction> streamJunctionMap,
                                Partition partition, int partitionIndex, SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
        if (partition.getPartitionTypeMap().isEmpty()) {
            throw new SiddhiAppCreationException("Partition must have at least one partition executor. " +
                    "But found none.");
        }
        try {
            Element element = AnnotationHelper.getAnnotationElement("info", "name",
                    partition.getAnnotations());
            if (element != null) {
                this.partitionName = element.getValue();
            }
        } catch (DuplicateAnnotationException e) {
            throw new DuplicateAnnotationException(e.getMessageWithOutContext() + " for the same Query " +
                    partition.toString(), e, e.getQueryContextStartIndex(), e.getQueryContextEndIndex(),
                    siddhiAppContext.getName(), siddhiAppContext.getSiddhiAppString());
        }
        if (partitionName == null) {
            this.partitionName = "partition_" + partitionIndex;
        }

        Annotation purge = AnnotationHelper.getAnnotation(SiddhiConstants.NAMESPACE_PURGE, partition.getAnnotations());
        if (purge != null) {
            if (purge.getElement(SiddhiConstants.ANNOTATION_ELEMENT_ENABLE) != null) {
                String purgeEnable = purge.getElement(SiddhiConstants.ANNOTATION_ELEMENT_ENABLE);
                if (!("true".equalsIgnoreCase(purgeEnable) || "false".equalsIgnoreCase(purgeEnable))) {
                    throw new SiddhiAppCreationException("Invalid value for enable: " + purgeEnable + "." +
                            " Please use 'true' or 'false'");
                } else {
                    purgingEnabled = Boolean.parseBoolean(purgeEnable);
                }
            } else {
                throw new SiddhiAppCreationException("Annotation @" + SiddhiConstants.NAMESPACE_PURGE +
                        " is missing element '" + SiddhiConstants.ANNOTATION_ELEMENT_ENABLE + "'");
            }
            if (purge.getElement(SiddhiConstants.ANNOTATION_ELEMENT_IDLE_PERIOD) != null) {
                String purgeIdle = purge.getElement(SiddhiConstants.ANNOTATION_ELEMENT_IDLE_PERIOD);
                purgeIdlePeriod = Expression.Time.timeToLong(purgeIdle);

            } else {
                throw new SiddhiAppCreationException("Annotation @" + SiddhiConstants.NAMESPACE_PURGE +
                        " is missing element '" + SiddhiConstants.ANNOTATION_ELEMENT_IDLE_PERIOD + "'");
            }

            if (purge.getElement(SiddhiConstants.ANNOTATION_ELEMENT_INTERVAL) != null) {
                String interval = purge.getElement(SiddhiConstants.ANNOTATION_ELEMENT_INTERVAL);
                purgeExecutionInterval = Expression.Time.timeToLong(interval);
            }
        }
        this.partition = partition;
        this.streamDefinitionMap = streamDefinitionMap;
        this.windowDefinitionMap = windowDefinitionMap;
        this.streamJunctionMap = streamJunctionMap;

        this.stateHolder = siddhiAppContext.generateStateHolder(partitionName, () -> new PartitionState());
    }

    public void addQuery(QueryRuntimeImpl metaQueryRuntime) {
        Query query = metaQueryRuntime.getQuery();

        if (query.getOutputStream() instanceof InsertIntoStream &&
                metaQueryRuntime.getOutputCallback() instanceof InsertIntoStreamCallback) {
            InsertIntoStreamCallback insertIntoStreamCallback = (InsertIntoStreamCallback) metaQueryRuntime
                    .getOutputCallback();
            StreamDefinition streamDefinition = insertIntoStreamCallback.getOutputStreamDefinition();
            String id = streamDefinition.getId();

            if (((InsertIntoStream) query.getOutputStream()).isInnerStream()) {
                metaQueryRuntime.setToLocalStream(true);
                localStreamDefinitionMap.putIfAbsent(id, streamDefinition);
                DefinitionParserHelper.validateOutputStream(streamDefinition, localStreamDefinitionMap.get(id));

                StreamJunction outputStreamJunction = localStreamJunctionMap.get(id);

                if (outputStreamJunction == null) {
                    outputStreamJunction = new StreamJunction(streamDefinition,
                            siddhiAppContext.getExecutorService(),
                            siddhiAppContext.getBufferSize(),
                            null, siddhiAppContext);
                    localStreamJunctionMap.putIfAbsent(id, outputStreamJunction);
                }
                insertIntoStreamCallback.init(localStreamJunctionMap.get(id));
            } else {
                streamDefinitionMap.putIfAbsent(id, streamDefinition);
                DefinitionParserHelper.validateOutputStream(streamDefinition, streamDefinitionMap.get(id));
                StreamJunction outputStreamJunction = streamJunctionMap.get(id);

                if (outputStreamJunction == null) {
                    outputStreamJunction = new StreamJunction(streamDefinition,
                            siddhiAppContext.getExecutorService(),
                            siddhiAppContext.getBufferSize(),
                            null, siddhiAppContext);
                    streamJunctionMap.putIfAbsent(id, outputStreamJunction);
                }
                insertIntoStreamCallback.init(streamJunctionMap.get(id));
            }
        } else if (query.getOutputStream() instanceof InsertIntoStream &&
                metaQueryRuntime.getOutputCallback() instanceof InsertIntoWindowCallback) {
            InsertIntoWindowCallback insertIntoWindowCallback = (InsertIntoWindowCallback)
                    metaQueryRuntime.getOutputCallback();
            StreamDefinition streamDefinition = insertIntoWindowCallback.getOutputStreamDefinition();
            String id = streamDefinition.getId();
            DefinitionParserHelper.validateOutputStream(streamDefinition, windowDefinitionMap.get(id));
            StreamJunction outputStreamJunction = streamJunctionMap.get(id);

            if (outputStreamJunction == null) {
                outputStreamJunction = new StreamJunction(streamDefinition,
                        siddhiAppContext.getExecutorService(),
                        siddhiAppContext.getBufferSize(),
                        null, siddhiAppContext);
                streamJunctionMap.putIfAbsent(id, outputStreamJunction);
            }
            insertIntoWindowCallback.getWindow().setPublisher(streamJunctionMap.get(insertIntoWindowCallback
                    .getOutputStreamDefinition().getId()).constructPublisher());
        }

        if (metaQueryRuntime.isFromLocalStream()) {
            for (int i = 0; i < metaQueryRuntime.getStreamRuntime().getSingleStreamRuntimes().size(); i++) {
                String streamId = metaQueryRuntime.getStreamRuntime().getSingleStreamRuntimes().get(i)
                        .getProcessStreamReceiver().getStreamId();
                if (streamId.startsWith("#")) {
                    StreamDefinition streamDefinition = (StreamDefinition) localStreamDefinitionMap.get(streamId);
                    StreamJunction streamJunction = localStreamJunctionMap.get(streamId);
                    if (streamJunction == null) {
                        streamJunction = new StreamJunction(streamDefinition, siddhiAppContext
                                .getExecutorService(),
                                siddhiAppContext.getBufferSize(),
                                null, siddhiAppContext);
                        localStreamJunctionMap.put(streamId, streamJunction);
                    }
                    streamJunction.subscribe(metaQueryRuntime.getStreamRuntime().getSingleStreamRuntimes().get
                            (i).getProcessStreamReceiver());
                }
            }
        }

        queryRuntimeList.add(metaQueryRuntime);
    }

    public void addPartitionReceiver(QueryRuntimeImpl queryRuntime, List<VariableExpressionExecutor> executors,
                                     MetaStateEvent metaEvent) {
        Query query = queryRuntime.getQuery();
        List<List<PartitionExecutor>> partitionExecutors = new StreamPartitioner(query.getInputStream(),
                partition, metaEvent, executors, queryRuntime.getSiddhiQueryContext()).getPartitionExecutorLists();
        if (queryRuntime.getStreamRuntime() instanceof SingleStreamRuntime) {
            SingleInputStream singleInputStream = (SingleInputStream) query.getInputStream();
            addPartitionReceiver(singleInputStream.getStreamId(), singleInputStream.isInnerStream(), metaEvent
                    .getMetaStreamEvent(0), partitionExecutors.get(0));
        } else if (queryRuntime.getStreamRuntime() instanceof JoinStreamRuntime) {
            SingleInputStream leftSingleInputStream = (SingleInputStream) ((JoinInputStream) query.getInputStream())
                    .getLeftInputStream();
            addPartitionReceiver(leftSingleInputStream.getStreamId(), leftSingleInputStream.isInnerStream(),
                    metaEvent.getMetaStreamEvent(0), partitionExecutors.get(0));
            SingleInputStream rightSingleInputStream = (SingleInputStream) ((JoinInputStream) query.getInputStream())
                    .getRightInputStream();
            addPartitionReceiver(rightSingleInputStream.getStreamId(), rightSingleInputStream.isInnerStream(),
                    metaEvent.getMetaStreamEvent(1), partitionExecutors.get(1));
        } else if (queryRuntime.getStreamRuntime() instanceof StateStreamRuntime) {
            StateElement stateElement = ((StateInputStream) query.getInputStream()).getStateElement();
            addPartitionReceiverForStateElement(stateElement, metaEvent, partitionExecutors, 0);
        }
    }

    private int addPartitionReceiverForStateElement(StateElement stateElement, MetaStateEvent metaEvent,
                                                    List<List<PartitionExecutor>> partitionExecutors,
                                                    int executorIndex) {
        if (stateElement instanceof EveryStateElement) {
            return addPartitionReceiverForStateElement(((EveryStateElement) stateElement).getStateElement(),
                    metaEvent, partitionExecutors, executorIndex);
        } else if (stateElement instanceof NextStateElement) {
            executorIndex = addPartitionReceiverForStateElement(((NextStateElement) stateElement).getStateElement(),
                    metaEvent, partitionExecutors, executorIndex);
            return addPartitionReceiverForStateElement(((NextStateElement) stateElement).getNextStateElement(),
                    metaEvent, partitionExecutors, executorIndex);
        } else if (stateElement instanceof CountStateElement) {
            return addPartitionReceiverForStateElement(((CountStateElement) stateElement).getStreamStateElement(),
                    metaEvent, partitionExecutors, executorIndex);
        } else if (stateElement instanceof LogicalStateElement) {
            executorIndex = addPartitionReceiverForStateElement(((LogicalStateElement) stateElement)
                            .getStreamStateElement1(), metaEvent,
                    partitionExecutors, executorIndex);
            return addPartitionReceiverForStateElement(((LogicalStateElement) stateElement).getStreamStateElement2(),
                    metaEvent, partitionExecutors, executorIndex);
        } else {  //if stateElement is an instanceof StreamStateElement
            SingleInputStream singleInputStream = ((StreamStateElement) stateElement).getBasicSingleInputStream();
            addPartitionReceiver(singleInputStream.getStreamId(), singleInputStream.isInnerStream(), metaEvent
                    .getMetaStreamEvent(executorIndex), partitionExecutors.get(executorIndex));
            return ++executorIndex;
        }
    }

    private void addPartitionReceiver(String streamId, boolean isInnerStream, MetaStreamEvent metaStreamEvent,
                                      List<PartitionExecutor> partitionExecutors) {
        if (!partitionStreamReceivers.containsKey(streamId) && !isInnerStream &&
                metaStreamEvent.getEventType() == MetaStreamEvent.EventType.DEFAULT) {
            StreamDefinition streamDefinition = (StreamDefinition) streamDefinitionMap.get(streamId);
            if (streamDefinition == null) {
                streamDefinition = (StreamDefinition) windowDefinitionMap.get(streamId);
            }
            PartitionStreamReceiver partitionStreamReceiver = new PartitionStreamReceiver(
                    siddhiAppContext, metaStreamEvent, streamDefinition, partitionExecutors, this);
            partitionStreamReceivers.put(partitionStreamReceiver.getStreamId(), partitionStreamReceiver);
            streamJunctionMap.get(partitionStreamReceiver.getStreamId()).subscribe(partitionStreamReceiver);
        }

    }

    private StreamJunction createStreamJunction(StreamDefinition streamDefinition) {
        return new StreamJunction(streamDefinition, siddhiAppContext.getExecutorService(),
                siddhiAppContext.getBufferSize(), null, siddhiAppContext);
    }

    public void addInnerpartitionStreamReceiverStreamJunction(String key, StreamJunction streamJunction) {
        innerPartitionStreamReceiverStreamJunctionMap.put(key, streamJunction);
    }

    public ConcurrentMap<String, StreamJunction> getInnerPartitionStreamReceiverStreamJunctionMap() {
        return innerPartitionStreamReceiverStreamJunctionMap;
    }

    public void init() {
        for (PartitionStreamReceiver partitionStreamReceiver : partitionStreamReceivers.values()) {
            partitionStreamReceiver.addStreamJunction(queryRuntimeList);
            partitionStreamReceiver.init();
        }
    }

    public String getPartitionName() {
        return partitionName;
    }

    public ConcurrentMap<String, AbstractDefinition> getLocalStreamDefinitionMap() {
        return localStreamDefinitionMap;
    }

    public ConcurrentMap<String, StreamJunction> getLocalStreamJunctionMap() {
        return localStreamJunctionMap;
    }


    public void setMemoryUsageTracker(MemoryUsageTracker memoryUsageTracker) {
        for (QueryRuntime queryRuntime : queryRuntimeList) {
            QueryParserHelper.registerMemoryUsageTracking(queryRuntime.getQueryId(), queryRuntime,
                    SiddhiConstants.METRIC_INFIX_QUERIES, siddhiAppContext, memoryUsageTracker);
        }
    }

    public void initPartition() {
        PartitionState state = stateHolder.getState();
        try {
            Long time = state.partitionKeys.get(SiddhiAppContext.getPartitionFlowId());
            if (time == null) {
                synchronized (state) {
                    time = state.partitionKeys.get(SiddhiAppContext.getPartitionFlowId());
                    if (time == null) {
                        for (QueryRuntime queryRuntime : queryRuntimeList) {
                            ((QueryRuntimeImpl) queryRuntime).initPartition();
                        }
                    }
                    state.partitionKeys.put(SiddhiAppContext.getPartitionFlowId(),
                            siddhiAppContext.getTimestampGenerator().currentTime());
                }
            } else {
                state.partitionKeys.put(SiddhiAppContext.getPartitionFlowId(),
                        siddhiAppContext.getTimestampGenerator().currentTime());
            }
        } finally {
            stateHolder.returnState(state);
        }
        if (purgingEnabled) {
            siddhiAppContext.getScheduledExecutorService().scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    long currentTime = siddhiAppContext.getTimestampGenerator().currentTime();
                    PartitionState state = stateHolder.getState();
                    try {
                        synchronized (state) {
                            HashMap<String, Long> partitions = new HashMap<>(state.partitionKeys);
                            for (Map.Entry<String, Long> partition : partitions.entrySet()) {
                                if (partition.getValue() + purgeIdlePeriod < currentTime) {
                                    state.partitionKeys.remove(partition.getKey());
                                    SiddhiAppContext.startPartitionFlow(partition.getKey());
                                    try {
                                        for (QueryRuntime queryRuntime : queryRuntimeList) {
                                            Map<String, StateHolder> elementHolderMap =
                                                    siddhiAppContext.getSnapshotService().getStateHolderMap(
                                                            partitionName, queryRuntime.getQueryId());
                                            for (StateHolder stateHolder : elementHolderMap.values()) {
                                                stateHolder.cleanGroupByStates();
                                            }
                                        }
                                    } finally {
                                        SiddhiAppContext.stopPartitionFlow();
                                    }
                                }
                            }
                        }
                    } finally {
                        stateHolder.returnState(state);
                    }
                }
            }, purgeExecutionInterval, purgeExecutionInterval, TimeUnit.MILLISECONDS);
        }
    }

    public Set<String> getPartitionKeys() {
        PartitionState state = stateHolder.getState();
        try {
            return new HashSet<>(state.partitionKeys.keySet());
        } finally {
            stateHolder.returnState(state);
        }
    }

    @Override
    public Collection<QueryRuntime> getQueries() {
        return queryRuntimeList;
    }

    /**
     * State of partition
     */
    public class PartitionState extends State {

        private Map<String, Long> partitionKeys = new ConcurrentHashMap<>();

        @Override
        public boolean canDestroy() {
            return partitionKeys.isEmpty();
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("PartitionKeys", partitionKeys);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            partitionKeys = (Map<String, Long>) state.get("PartitionKeys");
        }
    }

}
