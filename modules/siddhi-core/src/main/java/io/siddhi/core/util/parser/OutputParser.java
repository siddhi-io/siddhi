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
package io.siddhi.core.util.parser;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.state.StateEventFactory;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.event.stream.converter.StreamEventConverter;
import io.siddhi.core.event.stream.converter.ZeroStreamEventConverter;
import io.siddhi.core.exception.OperationNotSupportedException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.query.output.callback.DeleteTableCallback;
import io.siddhi.core.query.output.callback.InsertIntoStreamCallback;
import io.siddhi.core.query.output.callback.InsertIntoStreamEndPartitionCallback;
import io.siddhi.core.query.output.callback.InsertIntoTableCallback;
import io.siddhi.core.query.output.callback.InsertIntoWindowCallback;
import io.siddhi.core.query.output.callback.InsertIntoWindowEndPartitionCallback;
import io.siddhi.core.query.output.callback.OutputCallback;
import io.siddhi.core.query.output.callback.UpdateOrInsertTableCallback;
import io.siddhi.core.query.output.callback.UpdateTableCallback;
import io.siddhi.core.query.output.ratelimit.OutputRateLimiter;
import io.siddhi.core.query.output.ratelimit.PassThroughOutputRateLimiter;
import io.siddhi.core.query.output.ratelimit.event.AllPerEventOutputRateLimiter;
import io.siddhi.core.query.output.ratelimit.event.FirstGroupByPerEventOutputRateLimiter;
import io.siddhi.core.query.output.ratelimit.event.FirstPerEventOutputRateLimiter;
import io.siddhi.core.query.output.ratelimit.event.LastGroupByPerEventOutputRateLimiter;
import io.siddhi.core.query.output.ratelimit.event.LastPerEventOutputRateLimiter;
import io.siddhi.core.query.output.ratelimit.snapshot.WrappedSnapshotOutputRateLimiter;
import io.siddhi.core.query.output.ratelimit.time.AllPerTimeOutputRateLimiter;
import io.siddhi.core.query.output.ratelimit.time.FirstGroupByPerTimeOutputRateLimiter;
import io.siddhi.core.query.output.ratelimit.time.FirstPerTimeOutputRateLimiter;
import io.siddhi.core.query.output.ratelimit.time.LastGroupByPerTimeOutputRateLimiter;
import io.siddhi.core.query.output.ratelimit.time.LastPerTimeOutputRateLimiter;
import io.siddhi.core.stream.StreamJunction;
import io.siddhi.core.table.CompiledUpdateSet;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.parser.helper.DefinitionParserHelper;
import io.siddhi.core.window.Window;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import io.siddhi.query.api.execution.query.output.ratelimit.EventOutputRate;
import io.siddhi.query.api.execution.query.output.ratelimit.OutputRate;
import io.siddhi.query.api.execution.query.output.ratelimit.SnapshotOutputRate;
import io.siddhi.query.api.execution.query.output.ratelimit.TimeOutputRate;
import io.siddhi.query.api.execution.query.output.stream.DeleteStream;
import io.siddhi.query.api.execution.query.output.stream.InsertIntoStream;
import io.siddhi.query.api.execution.query.output.stream.OutputStream;
import io.siddhi.query.api.execution.query.output.stream.ReturnStream;
import io.siddhi.query.api.execution.query.output.stream.UpdateOrInsertStream;
import io.siddhi.query.api.execution.query.output.stream.UpdateSet;
import io.siddhi.query.api.execution.query.output.stream.UpdateStream;
import io.siddhi.query.api.expression.Variable;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Class to parse {@link OutputCallback}
 */
public class OutputParser {


    public static OutputCallback constructOutputCallback(final OutputStream outStream,
                                                         StreamDefinition outputStreamDefinition,
                                                         Map<String, Table> tableMap,
                                                         Map<String, Window> eventWindowMap,
                                                         boolean convertToStreamEvent, SiddhiQueryContext siddhiQueryContext) {
        String id = outStream.getId();
        Table table = null;
        Window window = null;
        if (id != null) {
            table = tableMap.get(id);
            window = eventWindowMap.get(id);
        }
        StreamEventFactory streamEventFactory = null;
        StreamEventConverter streamEventConverter = null;
        MetaStreamEvent tableMetaStreamEvent = null;
        if (table != null) {
            tableMetaStreamEvent = new MetaStreamEvent();
            tableMetaStreamEvent.setEventType(MetaStreamEvent.EventType.TABLE);
            TableDefinition matchingTableDefinition = TableDefinition.id("");
            for (Attribute attribute : outputStreamDefinition.getAttributeList()) {
                tableMetaStreamEvent.addOutputData(attribute);
                matchingTableDefinition.attribute(attribute.getName(), attribute.getType());
            }
            matchingTableDefinition.setQueryContextStartIndex(outStream.getQueryContextStartIndex());
            matchingTableDefinition.setQueryContextEndIndex(outStream.getQueryContextEndIndex());
            tableMetaStreamEvent.addInputDefinition(matchingTableDefinition);

            streamEventFactory = new StreamEventFactory(tableMetaStreamEvent);
            streamEventConverter = new ZeroStreamEventConverter();
        }

        //Construct CallBack
        if (outStream instanceof InsertIntoStream || outStream instanceof ReturnStream) {
            if (window != null) {
                if (!siddhiQueryContext.isPartitioned()) {
                    return new InsertIntoWindowCallback(window, outputStreamDefinition, siddhiQueryContext.getName());
                } else {
                    return new InsertIntoWindowEndPartitionCallback(window, outputStreamDefinition,
                            siddhiQueryContext.getName());
                }
            } else if (table != null) {
                DefinitionParserHelper.validateOutputStream(outputStreamDefinition, table.getTableDefinition());
                return new InsertIntoTableCallback(table, outputStreamDefinition, convertToStreamEvent,
                        streamEventFactory, streamEventConverter, siddhiQueryContext.getName());
            } else {
                if (!siddhiQueryContext.isPartitioned() || outputStreamDefinition.getId().startsWith("#")) {
                    return new InsertIntoStreamCallback(outputStreamDefinition, siddhiQueryContext.getName());
                } else {
                    return new InsertIntoStreamEndPartitionCallback(
                            outputStreamDefinition, siddhiQueryContext.getName());
                }
            }
        } else if (outStream instanceof DeleteStream || outStream instanceof UpdateStream || outStream instanceof
                UpdateOrInsertStream) {
            if (table != null) {

                if (outStream instanceof UpdateStream) {
                    if (((UpdateStream) outStream).getUpdateSet() == null) {
                        TableDefinition tableDefinition = table.getTableDefinition();
                        for (Attribute attribute : outputStreamDefinition.getAttributeList()) {
                            if (!tableDefinition.getAttributeList().contains(attribute)) {
                                throw new SiddhiAppCreationException("Attribute " + attribute + " does not exist on " +
                                        "Event Table " + tableDefinition, outStream.getQueryContextStartIndex(),
                                        outStream.getQueryContextEndIndex());
                            }
                        }
                    }
                }

                if (outStream instanceof UpdateOrInsertStream) {
                    TableDefinition tableDefinition = table.getTableDefinition();
                    for (Attribute attribute : outputStreamDefinition.getAttributeList()) {
                        if (!tableDefinition.getAttributeList().contains(attribute)) {
                            throw new SiddhiAppCreationException("Attribute " + attribute + " does not exist on " +
                                    "Event Table " + tableDefinition, outStream.getQueryContextStartIndex(),
                                    outStream.getQueryContextEndIndex());
                        }
                    }
                }

                if (outStream instanceof DeleteStream) {
                    try {
                        MatchingMetaInfoHolder matchingMetaInfoHolder =
                                MatcherParser.constructMatchingMetaStateHolder(tableMetaStreamEvent, 0,
                                        table.getTableDefinition(), 0);
                        CompiledCondition compiledCondition = table.compileCondition(
                                (((DeleteStream) outStream).getOnDeleteExpression()), matchingMetaInfoHolder,
                                null, tableMap, siddhiQueryContext);
                        StateEventFactory stateEventFactory = new StateEventFactory(
                                matchingMetaInfoHolder.getMetaStateEvent());
                        return new DeleteTableCallback(table, compiledCondition, matchingMetaInfoHolder.
                                getMatchingStreamEventIndex(), convertToStreamEvent, stateEventFactory, streamEventFactory,
                                streamEventConverter, siddhiQueryContext.getName());
                    } catch (SiddhiAppValidationException e) {
                        throw new SiddhiAppCreationException("Cannot create delete for table '" + outStream.getId() +
                                "', " + e.getMessageWithOutContext(), e, e.getQueryContextStartIndex(),
                                e.getQueryContextEndIndex(), siddhiQueryContext.getName(),
                                siddhiQueryContext.getSiddhiAppContext().getSiddhiAppString());
                    }
                } else if (outStream instanceof UpdateStream) {
                    try {
                        MatchingMetaInfoHolder matchingMetaInfoHolder =
                                MatcherParser.constructMatchingMetaStateHolder(tableMetaStreamEvent, 0,
                                        table.getTableDefinition(), 0);
                        CompiledCondition compiledCondition = table.compileCondition((((UpdateStream) outStream).
                                        getOnUpdateExpression()), matchingMetaInfoHolder, null,
                                tableMap, siddhiQueryContext);
                        UpdateSet updateSet = ((UpdateStream) outStream).getUpdateSet();
                        if (updateSet == null) {
                            updateSet = new UpdateSet();
                            for (Attribute attribute : matchingMetaInfoHolder.getMatchingStreamDefinition().
                                    getAttributeList()) {
                                updateSet.set(new Variable(attribute.getName()), new Variable(attribute.getName()));
                            }
                        }
                        CompiledUpdateSet compiledUpdateSet = table.compileUpdateSet(updateSet, matchingMetaInfoHolder,
                                null, tableMap, siddhiQueryContext);
                        StateEventFactory stateEventFactory = new StateEventFactory(
                                matchingMetaInfoHolder.getMetaStateEvent());
                        return new UpdateTableCallback(table, compiledCondition, compiledUpdateSet,
                                matchingMetaInfoHolder.getMatchingStreamEventIndex(), convertToStreamEvent,
                                stateEventFactory, streamEventFactory, streamEventConverter, siddhiQueryContext.getName());
                    } catch (SiddhiAppValidationException e) {
                        throw new SiddhiAppCreationException("Cannot create update for table '" + outStream.getId() +
                                "', " + e.getMessageWithOutContext(), e, e.getQueryContextStartIndex(),
                                e.getQueryContextEndIndex(), siddhiQueryContext.getSiddhiAppContext());
                    }
                } else {
                    DefinitionParserHelper.validateOutputStream(outputStreamDefinition, table.getTableDefinition
                            ());
                    try {
                        MatchingMetaInfoHolder matchingMetaInfoHolder =
                                MatcherParser.constructMatchingMetaStateHolder(tableMetaStreamEvent, 0,
                                        table.getTableDefinition(), 0);
                        CompiledCondition compiledCondition = table.
                                compileCondition((((UpdateOrInsertStream) outStream).getOnUpdateExpression()),
                                        matchingMetaInfoHolder, null, tableMap, siddhiQueryContext
                                );
                        UpdateSet updateSet = ((UpdateOrInsertStream) outStream).getUpdateSet();
                        if (updateSet == null) {
                            updateSet = new UpdateSet();
                            for (Attribute attribute : matchingMetaInfoHolder.getMatchingStreamDefinition().
                                    getAttributeList()) {
                                updateSet.set(new Variable(attribute.getName()), new Variable(attribute.getName()));
                            }
                        }
                        CompiledUpdateSet compiledUpdateSet = table.compileUpdateSet(updateSet, matchingMetaInfoHolder,
                                null, tableMap, siddhiQueryContext);
                        StateEventFactory stateEventFactory = new StateEventFactory(matchingMetaInfoHolder.getMetaStateEvent());
                        return new UpdateOrInsertTableCallback(table, compiledCondition, compiledUpdateSet,
                                matchingMetaInfoHolder.getMatchingStreamEventIndex(), convertToStreamEvent,
                                stateEventFactory, streamEventFactory, streamEventConverter, siddhiQueryContext.getName());

                    } catch (SiddhiAppValidationException e) {
                        throw new SiddhiAppCreationException("Cannot create update or insert into for table '" +
                                outStream.getId() + "', " + e.getMessageWithOutContext(), e,
                                e.getQueryContextStartIndex(), e.getQueryContextEndIndex(),
                                siddhiQueryContext.getSiddhiAppContext());
                    }
                }
            } else {
                throw new SiddhiAppCreationException("Event table with id :" + id + " does not exist",
                        outStream.getQueryContextStartIndex(),
                        outStream.getQueryContextEndIndex());
            }
        } else {
            throw new SiddhiAppCreationException(outStream.getClass().getName() + " not supported",
                    outStream.getQueryContextStartIndex(),
                    outStream.getQueryContextEndIndex());
        }

    }

    public static OutputCallback constructOutputCallback(OutputStream outStream, String key,
                                                         ConcurrentMap<String, StreamJunction> streamJunctionMap,
                                                         StreamDefinition outputStreamDefinition,
                                                         SiddhiQueryContext siddhiQueryContext) {
        String id = outStream.getId();
        //Construct CallBack
        if (outStream instanceof InsertIntoStream) {
            StreamJunction outputStreamJunction = streamJunctionMap.get(id + key);
            if (outputStreamJunction == null) {
                outputStreamJunction = new StreamJunction(outputStreamDefinition,
                        siddhiQueryContext.getSiddhiAppContext().getExecutorService(),
                        siddhiQueryContext.getSiddhiAppContext().getBufferSize(), null,
                        siddhiQueryContext.getSiddhiAppContext());
                streamJunctionMap.putIfAbsent(id + key, outputStreamJunction);
            }
            InsertIntoStreamCallback insertIntoStreamCallback = new InsertIntoStreamCallback(outputStreamDefinition,
                    siddhiQueryContext.getName());
            insertIntoStreamCallback.init(streamJunctionMap.get(id + key));
            return insertIntoStreamCallback;

        } else {
            throw new SiddhiAppCreationException(outStream.getClass().getName() + " not supported",
                    outStream.getQueryContextStartIndex(), outStream.getQueryContextEndIndex());
        }
    }

    public static OutputRateLimiter constructOutputRateLimiter(String id, OutputRate outputRate,
                                                               boolean isGroupBy,
                                                               boolean isWindow,
                                                               SiddhiQueryContext siddhiQueryContext) {
        if (outputRate == null) {
            return new PassThroughOutputRateLimiter(id);
        } else if (outputRate instanceof EventOutputRate) {
            switch (((EventOutputRate) outputRate).getType()) {
                case ALL:
                    return new AllPerEventOutputRateLimiter(id, ((EventOutputRate) outputRate).getValue());
                case FIRST:
                    if (isGroupBy) {
                        return new FirstGroupByPerEventOutputRateLimiter(id, ((EventOutputRate) outputRate).getValue());
                    } else {
                        return new FirstPerEventOutputRateLimiter(id, ((EventOutputRate) outputRate).getValue());
                    }
                case LAST:
                    if (isGroupBy) {
                        return new LastGroupByPerEventOutputRateLimiter(id, ((EventOutputRate) outputRate).getValue());
                    } else {
                        return new LastPerEventOutputRateLimiter(id, ((EventOutputRate) outputRate).getValue());
                    }
            }
            //never happens
            throw new OperationNotSupportedException(((EventOutputRate) outputRate).getType() + " not supported in " +
                    "output rate limiting");
        } else if (outputRate instanceof TimeOutputRate) {
            switch (((TimeOutputRate) outputRate).getType()) {
                case ALL:
                    return new AllPerTimeOutputRateLimiter(id, ((TimeOutputRate) outputRate).getValue());
                case FIRST:
                    if (isGroupBy) {
                        return new FirstGroupByPerTimeOutputRateLimiter(id, ((TimeOutputRate) outputRate).getValue());
                    } else {
                        return new FirstPerTimeOutputRateLimiter(id, ((TimeOutputRate) outputRate).getValue());
                    }
                case LAST:
                    if (isGroupBy) {
                        return new LastGroupByPerTimeOutputRateLimiter(id, ((TimeOutputRate) outputRate).getValue());
                    } else {
                        return new LastPerTimeOutputRateLimiter(id, ((TimeOutputRate) outputRate).getValue());
                    }
            }
            //never happens
            throw new OperationNotSupportedException(((TimeOutputRate) outputRate).getType() + " not supported in " +
                    "output rate limiting");
        } else {
            return new WrappedSnapshotOutputRateLimiter(((SnapshotOutputRate) outputRate).getValue(),
                    isGroupBy, isWindow, siddhiQueryContext);
        }

    }


}
