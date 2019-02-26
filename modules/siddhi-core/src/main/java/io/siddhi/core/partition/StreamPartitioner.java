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

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.MetaComplexEvent;
import io.siddhi.core.event.state.MetaStateEvent;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.executor.condition.ConditionExpressionExecutor;
import io.siddhi.core.partition.executor.PartitionExecutor;
import io.siddhi.core.partition.executor.RangePartitionExecutor;
import io.siddhi.core.partition.executor.ValuePartitionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.parser.ExpressionParser;
import io.siddhi.query.api.execution.partition.Partition;
import io.siddhi.query.api.execution.partition.PartitionType;
import io.siddhi.query.api.execution.partition.RangePartitionType;
import io.siddhi.query.api.execution.partition.ValuePartitionType;
import io.siddhi.query.api.execution.query.input.state.CountStateElement;
import io.siddhi.query.api.execution.query.input.state.EveryStateElement;
import io.siddhi.query.api.execution.query.input.state.LogicalStateElement;
import io.siddhi.query.api.execution.query.input.state.NextStateElement;
import io.siddhi.query.api.execution.query.input.state.StateElement;
import io.siddhi.query.api.execution.query.input.state.StreamStateElement;
import io.siddhi.query.api.execution.query.input.stream.InputStream;
import io.siddhi.query.api.execution.query.input.stream.JoinInputStream;
import io.siddhi.query.api.execution.query.input.stream.SingleInputStream;
import io.siddhi.query.api.execution.query.input.stream.StateInputStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * create PartitionExecutors to be used to get partitioning key
 */
public class StreamPartitioner {

    private List<List<PartitionExecutor>> partitionExecutorLists = new ArrayList<List<PartitionExecutor>>();

    public StreamPartitioner(InputStream inputStream, Partition partition, MetaStateEvent metaEvent,
                             List<VariableExpressionExecutor> executors, SiddhiQueryContext siddhiQueryContext) {
        if (partition != null) {
            createExecutors(inputStream, partition, metaEvent, executors,
                    siddhiQueryContext);
        }
    }

    private void createExecutors(InputStream inputStream, Partition partition, MetaComplexEvent metaEvent,
                                 List<VariableExpressionExecutor> executors, SiddhiQueryContext siddhiQueryContext) {
        if (inputStream instanceof SingleInputStream) {
            if (metaEvent instanceof MetaStateEvent) {
                createSingleInputStreamExecutors((SingleInputStream) inputStream, partition,
                        ((MetaStateEvent) metaEvent).getMetaStreamEvent(0), executors, null,
                        siddhiQueryContext);
            } else {
                createSingleInputStreamExecutors((SingleInputStream) inputStream, partition, (MetaStreamEvent)
                        metaEvent, executors, null, siddhiQueryContext);
            }
        } else if (inputStream instanceof JoinInputStream) {
            createJoinInputStreamExecutors((JoinInputStream) inputStream, partition, (MetaStateEvent) metaEvent,
                    executors, siddhiQueryContext);
        } else if (inputStream instanceof StateInputStream) {
            createStateInputStreamExecutors(((StateInputStream) inputStream).getStateElement(), partition,
                    (MetaStateEvent) metaEvent, executors, 0, siddhiQueryContext);
        }
    }

    private int createStateInputStreamExecutors(StateElement stateElement, Partition partition, MetaStateEvent
            metaEvent, List<VariableExpressionExecutor> executors,
                                                int executorIndex, SiddhiQueryContext siddhiQueryContext) {

        if (stateElement instanceof EveryStateElement) {
            return createStateInputStreamExecutors(((EveryStateElement) stateElement).getStateElement(), partition,
                    metaEvent, executors, executorIndex, siddhiQueryContext);
        } else if (stateElement instanceof NextStateElement) {
            executorIndex = createStateInputStreamExecutors(((NextStateElement) stateElement).getStateElement(),
                    partition, metaEvent, executors, executorIndex, siddhiQueryContext);
            return createStateInputStreamExecutors(((NextStateElement) stateElement).getNextStateElement(),
                    partition, metaEvent, executors, executorIndex, siddhiQueryContext);
        } else if (stateElement instanceof LogicalStateElement) {
            executorIndex = createStateInputStreamExecutors(((LogicalStateElement) stateElement)
                            .getStreamStateElement1(), partition, metaEvent, executors, executorIndex,
                    siddhiQueryContext);
            return createStateInputStreamExecutors(((LogicalStateElement) stateElement).getStreamStateElement2(),
                    partition, metaEvent, executors, executorIndex, siddhiQueryContext);
        } else if (stateElement instanceof CountStateElement) {
            return createStateInputStreamExecutors(((CountStateElement) stateElement).getStreamStateElement(),
                    partition, metaEvent, executors, executorIndex, siddhiQueryContext);
        } else {  //when stateElement is an instanceof StreamStateElement
            int size = executors.size();
            createExecutors(((StreamStateElement) stateElement).getBasicSingleInputStream(), partition, metaEvent
                    .getMetaStreamEvent(executorIndex), executors, siddhiQueryContext);
            for (int j = size; j < executors.size(); j++) {
                executors.get(j).getPosition()[SiddhiConstants.STREAM_EVENT_CHAIN_INDEX] = executorIndex;
            }
            return ++executorIndex;
        }
    }

    private void createJoinInputStreamExecutors(JoinInputStream inputStream, Partition partition,
                                                MetaStateEvent metaEvent, List<VariableExpressionExecutor> executors,
                                                SiddhiQueryContext siddhiQueryContext) {
        createExecutors(inputStream.getLeftInputStream(), partition, metaEvent.getMetaStreamEvent(0), executors,
                siddhiQueryContext);
        int size = executors.size();
        for (VariableExpressionExecutor variableExpressionExecutor : executors) {
            variableExpressionExecutor.getPosition()[SiddhiConstants.STREAM_EVENT_CHAIN_INDEX] = 0;
        }
        createExecutors(inputStream.getRightInputStream(), partition, metaEvent.getMetaStreamEvent(1), executors,
                siddhiQueryContext);
        for (int i = size; i < executors.size(); i++) {
            executors.get(i).getPosition()[SiddhiConstants.STREAM_EVENT_CHAIN_INDEX] = 1;
        }

    }

    private void createSingleInputStreamExecutors(SingleInputStream inputStream, Partition partition,
                                                  MetaStreamEvent metaEvent,
                                                  List<VariableExpressionExecutor> executors,
                                                  Map<String, Table> tableMap,
                                                  SiddhiQueryContext siddhiQueryContext) {
        List<PartitionExecutor> executorList = new ArrayList<PartitionExecutor>();
        partitionExecutorLists.add(executorList);
        if (!inputStream.isInnerStream()) {
            for (PartitionType partitionType : partition.getPartitionTypeMap().values()) {
                if (partitionType instanceof ValuePartitionType) {
                    if (partitionType.getStreamId().equals(inputStream.getStreamId())) {
                        executorList.add(new ValuePartitionExecutor(ExpressionParser.parseExpression((
                                        (ValuePartitionType) partitionType).getExpression(),
                                metaEvent, SiddhiConstants.UNKNOWN_STATE, tableMap, executors,
                                false, 0,
                                ProcessingMode.BATCH, false, siddhiQueryContext)));
                    }
                } else {
                    for (RangePartitionType.RangePartitionProperty rangePartitionProperty : ((RangePartitionType)
                            partitionType).getRangePartitionProperties()) {
                        if (partitionType.getStreamId().equals(inputStream.getStreamId())) {
                            executorList.add(new RangePartitionExecutor((ConditionExpressionExecutor)
                                    ExpressionParser.parseExpression(rangePartitionProperty.getCondition(), metaEvent,
                                            SiddhiConstants.UNKNOWN_STATE, tableMap, executors,
                                            false, 0, ProcessingMode.BATCH,
                                            false, siddhiQueryContext),
                                    rangePartitionProperty.getPartitionKey()));
                        }
                    }
                }
            }
        }
    }

    public List<List<PartitionExecutor>> getPartitionExecutorLists() {
        return partitionExecutorLists;
    }


}
