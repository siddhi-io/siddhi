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
package io.siddhi.core.query.input.stream.join;

import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.state.StateEventFactory;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.processor.stream.window.FindableProcessor;
import io.siddhi.core.query.processor.stream.window.QueryableProcessor;
import io.siddhi.core.query.processor.stream.window.TableWindowProcessor;
import io.siddhi.core.query.selector.QuerySelector;
import io.siddhi.core.query.selector.SelectorTypeComplexEventChunk;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.CompiledSelection;
import io.siddhi.query.api.definition.Attribute;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.List;

/**
 * Created on 12/8/14.
 */
public class JoinProcessor implements Processor {
    private static final Logger log = LogManager.getLogger(JoinProcessor.class);
    private boolean trigger;
    private boolean leftJoinProcessor = false;
    private boolean outerJoinProcessor = false;
    private int matchingStreamIndex;
    private boolean preJoinProcessor;
    private StateEventFactory stateEventFactory;
    private CompiledCondition compiledCondition;
    private CompiledSelection compiledSelection;
    private boolean isOptimisedQuery;
    private Attribute[] expectedOutputAttributes;
    private FindableProcessor findableProcessor;
    private Processor nextProcessor;
    private QuerySelector selector;
    private String siddhiAppName;
    private String queryName;

    public JoinProcessor(boolean leftJoinProcessor, boolean preJoinProcessor, boolean outerJoinProcessor,
                         int matchingStreamIndex, String siddhiAppName, String queryName) {
        this.leftJoinProcessor = leftJoinProcessor;
        this.preJoinProcessor = preJoinProcessor;
        this.outerJoinProcessor = outerJoinProcessor;
        this.matchingStreamIndex = matchingStreamIndex;
        this.siddhiAppName = siddhiAppName;
        this.queryName = queryName;
    }

    /**
     * Process the handed StreamEvent.
     *
     * @param complexEventChunk event chunk to be processed
     */
    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        if (trigger) {
            List<ComplexEventChunk> returnEventChunkList = new LinkedList<>();
            execute(complexEventChunk, returnEventChunkList);
            selector.process(returnEventChunkList);
        } else {
            if (preJoinProcessor) {
                nextProcessor.process(complexEventChunk);
            }
        }
    }

    @Override
    public void process(List<ComplexEventChunk> complexEventChunks) {
        if (trigger) {
            List<ComplexEventChunk> returnEventChunkList = new LinkedList<>();
            for (ComplexEventChunk streamEventChunk : complexEventChunks) {
                execute(streamEventChunk, returnEventChunkList);
            }
            selector.process(returnEventChunkList);
        } else {
            if (preJoinProcessor) {
                nextProcessor.process(complexEventChunks);
            }
        }
    }

    private void execute(ComplexEventChunk complexEventChunk,
                         List<ComplexEventChunk> returnEventChunkList) {
        StateEvent joinStateEvent = new StateEvent(2, 0);
        StreamEvent nextEvent = (StreamEvent) complexEventChunk.getFirst();
        complexEventChunk.clear();
        while (nextEvent != null) {
            StreamEvent streamEvent = nextEvent;
            nextEvent = streamEvent.getNext();
            streamEvent.setNext(null);
            ComplexEvent.Type eventType = streamEvent.getType();
            if (eventType == ComplexEvent.Type.TIMER) {
                continue;
            } else if (eventType == ComplexEvent.Type.RESET) {
                if (!leftJoinProcessor) {
                    StateEvent outputStateEvent = joinEventBuilder(null, streamEvent, eventType);
                    returnEventChunkList.add(new SelectorTypeComplexEventChunk(new ComplexEventChunk<>(
                            outputStateEvent, outputStateEvent), false));
                } else {
                    StateEvent outputStateEvent = joinEventBuilder(streamEvent, null, eventType);
                    returnEventChunkList.add(new SelectorTypeComplexEventChunk(new ComplexEventChunk<>(
                            outputStateEvent, outputStateEvent), false));
                }
            } else {
                joinStateEvent.setEvent(matchingStreamIndex, streamEvent);

                StreamEvent foundStreamEvent;
                if (this.isOptimisedQuery) {
                    try {
                        foundStreamEvent = query(joinStateEvent);
                    } catch (SiddhiAppRuntimeException e) {
                        log.warn("Performing select clause in databases failed due to '" + e.getMessage() +
                                " in query '" + queryName + "' within Siddhi app '" + siddhiAppName +
                                "' hence reverting back to querying only with where clause.", e);
                        this.isOptimisedQuery = false;
                        foundStreamEvent = findableProcessor.find(joinStateEvent, compiledCondition);
                    }
                } else {
                    foundStreamEvent = findableProcessor.find(joinStateEvent, compiledCondition);
                }

                joinStateEvent.setEvent(matchingStreamIndex, null);
                if (foundStreamEvent == null) {
                    if (outerJoinProcessor && !leftJoinProcessor) {
                        StateEvent outputStateEvent = joinEventBuilder(null, streamEvent, eventType);
                        returnEventChunkList.add(new SelectorTypeComplexEventChunk(new ComplexEventChunk<>(
                                outputStateEvent, outputStateEvent), false));
                    } else if (outerJoinProcessor && leftJoinProcessor) {
                        StateEvent outputStateEvent = joinEventBuilder(streamEvent, null, eventType);
                        returnEventChunkList.add(new SelectorTypeComplexEventChunk(new ComplexEventChunk<>(
                                outputStateEvent, outputStateEvent), false));
                    }
                } else if (!isOptimisedQuery) {
                    ComplexEventChunk<ComplexEvent> returnEventChunk = new ComplexEventChunk<>();
                    while (foundStreamEvent != null) {
                        StreamEvent nextFoundStreamEvent = foundStreamEvent.getNext();
                        foundStreamEvent.setNext(null);
                        if (!leftJoinProcessor) {
                            returnEventChunk.add(joinEventBuilder(foundStreamEvent, streamEvent, eventType));
                        } else {
                            returnEventChunk.add(joinEventBuilder(streamEvent, foundStreamEvent, eventType));
                        }
                        foundStreamEvent = nextFoundStreamEvent;
                    }
                    returnEventChunkList.add(new SelectorTypeComplexEventChunk(returnEventChunk, false));
                } else {
                    ComplexEventChunk<ComplexEvent> returnEventChunk = new ComplexEventChunk<>();
                    while (foundStreamEvent != null) {
                        StreamEvent nextFoundStreamEvent = foundStreamEvent.getNext();
                        StateEvent returnEvent = stateEventFactory.newInstance();
                        returnEvent.setType(eventType);
                        returnEvent.setTimestamp(foundStreamEvent.getTimestamp());
                        Object[] outputData = foundStreamEvent.getOutputData();
                        for (int i = 0; i < outputData.length; i++) {
                            Object data = outputData[i];
                            returnEvent.setOutputData(data, i);
                        }
                        returnEventChunk.add(returnEvent);
                        foundStreamEvent = nextFoundStreamEvent;
                    }
                    returnEventChunkList.add(new SelectorTypeComplexEventChunk(returnEventChunk, true));
                }
            }
        }
    }

    private StreamEvent query(StateEvent joinStateEvent) throws SiddhiAppRuntimeException {
        Table table = ((TableWindowProcessor) findableProcessor).getTable();
        if (table.getIsConnected()) {
            try {
                return ((QueryableProcessor) findableProcessor).query(joinStateEvent, compiledCondition,
                        compiledSelection, expectedOutputAttributes);
            } catch (ConnectionUnavailableException e) {
                table.setIsConnectedToFalse();
                table.connectWithRetry();
                return query(joinStateEvent);
            }
        } else if (table.getIsTryingToConnect()) {
            log.warn("Error while performing query '" + queryName + "' within Siddhi app '" + siddhiAppName +
                    "' for event '" + joinStateEvent + "', operation busy waiting at Table '" +
                    table.getTableDefinition().getId() + "' as its trying to reconnect!");
            table.waitWhileConnect();
            log.info("Table '" + table.getTableDefinition().getId() + "' has become available for query '" +
                    queryName + "' within Siddhi app '" + siddhiAppName + "for matching event '" +
                    joinStateEvent + "'");
            return query(joinStateEvent);
        } else {
            table.connectWithRetry();
            return query(joinStateEvent);
        }
    }

    public void setExpectedOutputAttributes(List<Attribute> expectedOutputAttributes) {
        this.expectedOutputAttributes = expectedOutputAttributes.toArray(new Attribute[0]);
    }

    /**
     * Get next processor element in the processor chain. Processed event should be sent to next processor
     *
     * @return Next Processor
     */
    @Override
    public Processor getNextProcessor() {
        return nextProcessor;
    }

    /**
     * Set next processor element in processor chain.
     *
     * @param processor Processor to be set as next element of processor chain
     */
    @Override
    public void setNextProcessor(Processor processor) {
        nextProcessor = processor;
    }

    /**
     * Set as the last element of the processor chain.
     *
     * @param processor Last processor in the chain
     */
    @Override
    public void setToLast(Processor processor) {
        if (nextProcessor == null) {
            this.nextProcessor = processor;
        } else {
            this.nextProcessor.setToLast(processor);
        }
        if (processor instanceof QuerySelector) {
            selector = (QuerySelector) processor;
        }
    }

    public void setFindableProcessor(FindableProcessor findableProcessor) {
        this.findableProcessor = findableProcessor;
    }

    public CompiledCondition getCompiledCondition() {
        return this.compiledCondition;
    }

    public void setCompiledCondition(CompiledCondition compiledCondition) {
        this.compiledCondition = compiledCondition;
    }

    public void setTrigger(boolean trigger) {
        this.trigger = trigger;
    }

    public void setStateEventFactory(StateEventFactory stateEventFactory) {
        this.stateEventFactory = stateEventFactory;
    }

    public CompiledSelection getCompiledSelection() {
        return compiledSelection;
    }

    public void setCompiledSelection(CompiledSelection compiledSelection) {
        if (compiledSelection != null) {
            this.isOptimisedQuery = true;
            this.compiledSelection = compiledSelection;
        }
    }

    /**
     * Join the given two event streams.
     *
     * @param leftStream  event left stream
     * @param rightStream event right stream
     * @param type        complex event type
     * @return StateEvent state event
     */
    public StateEvent joinEventBuilder(StreamEvent leftStream, StreamEvent rightStream, ComplexEvent.Type type) {
        StateEvent returnEvent = stateEventFactory.newInstance();
        returnEvent.setEvent(0, leftStream);
        returnEvent.setEvent(1, rightStream);
        returnEvent.setType(type);
        if (!leftJoinProcessor) {
            returnEvent.setTimestamp(rightStream.getTimestamp());
        } else {
            returnEvent.setTimestamp(leftStream.getTimestamp());
        }
        return returnEvent;
    }

}
