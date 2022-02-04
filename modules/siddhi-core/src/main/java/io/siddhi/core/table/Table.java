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

package io.siddhi.core.table;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventCloner;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.DatabaseRuntimeException;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.stream.window.FindableProcessor;
import io.siddhi.core.table.record.RecordTableHandler;
import io.siddhi.core.util.ExceptionUtil;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.StringUtil;
import io.siddhi.core.util.collection.AddingStreamEventExtractor;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.error.handler.model.ErroneousEvent;
import io.siddhi.core.util.error.handler.model.ReplayableTableRecord;
import io.siddhi.core.util.error.handler.util.ErrorHandlerUtils;
import io.siddhi.core.util.error.handler.util.ErrorOccurrence;
import io.siddhi.core.util.error.handler.util.ErrorStoreHelper;
import io.siddhi.core.util.parser.helper.QueryParserHelper;
import io.siddhi.core.util.statistics.LatencyTracker;
import io.siddhi.core.util.statistics.MemoryCalculable;
import io.siddhi.core.util.statistics.ThroughputTracker;
import io.siddhi.core.util.statistics.metrics.Level;
import io.siddhi.core.util.transport.BackoffRetryCounter;
import io.siddhi.query.api.annotation.Element;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.execution.query.output.stream.UpdateSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.siddhi.core.util.SiddhiConstants.ANNOTATION_ELEMENT_ON_ERROR;
import static io.siddhi.core.util.SiddhiConstants.ANNOTATION_STORE;
import static io.siddhi.query.api.util.AnnotationHelper.getAnnotationElement;

/**
 * Interface class to represent Tables in Siddhi. There are multiple implementations. Ex: {@link InMemoryTable}. Table
 * will support basic operations of add, delete, update, update or add and contains. *
 */
public abstract class Table implements FindableProcessor, MemoryCalculable {

    private static final Logger LOG = LogManager.getLogger(Table.class);
    public Map<String, Table> tableMap;
    protected TableDefinition tableDefinition;
    protected SiddhiAppContext siddhiAppContext;
    private AtomicBoolean isTryingToConnect = new AtomicBoolean(false);
    private BackoffRetryCounter backoffRetryCounter = new BackoffRetryCounter();
    private AtomicBoolean isConnected = new AtomicBoolean(false);
    private ScheduledExecutorService scheduledExecutorService;
    private RecordTableHandler recordTableHandler;
    private OnErrorAction onErrorAction = OnErrorAction.RETRY;
    private boolean isObjectColumnPresent;
    private LatencyTracker latencyTrackerFind;
    private LatencyTracker latencyTrackerInsert;
    private LatencyTracker latencyTrackerUpdate;
    private LatencyTracker latencyTrackerDelete;
    private LatencyTracker latencyTrackerUpdateOrInsert;
    private LatencyTracker latencyTrackerContains;
    private ThroughputTracker throughputTrackerFind;
    private ThroughputTracker throughputTrackerInsert;
    private ThroughputTracker throughputTrackerUpdate;
    private ThroughputTracker throughputTrackerDelete;
    private ThroughputTracker throughputTrackerUpdateOrInsert;
    private ThroughputTracker throughputTrackerContains;

    public void initTable(TableDefinition tableDefinition, StreamEventFactory storeEventPool,
                          StreamEventCloner storeEventCloner,
                          ConfigReader configReader, SiddhiAppContext siddhiAppContext,
                          RecordTableHandler recordTableHandler) {
        this.tableDefinition = tableDefinition;
        this.scheduledExecutorService = siddhiAppContext.getScheduledExecutorService();
        this.siddhiAppContext = siddhiAppContext;
        this.recordTableHandler = recordTableHandler;
        Element onErrorElement = getAnnotationElement(ANNOTATION_STORE, ANNOTATION_ELEMENT_ON_ERROR,
                tableDefinition.getAnnotations());
        if (onErrorElement != null) {
            this.onErrorAction = OnErrorAction.valueOf(onErrorElement.getValue());
        }
        if (this.onErrorAction == OnErrorAction.STORE && siddhiAppContext.getSiddhiContext().getErrorStore() == null) {
            LOG.error("On error action is 'STORE' for table " + tableDefinition.getId() + " in Siddhi App "
                    + siddhiAppContext.getName() + " but error store is not configured in Siddhi Manager");
        }
        this.isObjectColumnPresent = isObjectColumnPresent(tableDefinition);
        if (siddhiAppContext.getStatisticsManager() != null) {
            latencyTrackerFind = QueryParserHelper.createLatencyTracker(siddhiAppContext, tableDefinition.getId(),
                    SiddhiConstants.METRIC_INFIX_TABLES, SiddhiConstants.METRIC_TYPE_FIND);
            latencyTrackerInsert = QueryParserHelper.createLatencyTracker(siddhiAppContext, tableDefinition.getId(),
                    SiddhiConstants.METRIC_INFIX_TABLES, SiddhiConstants.METRIC_TYPE_INSERT);
            latencyTrackerUpdate = QueryParserHelper.createLatencyTracker(siddhiAppContext, tableDefinition.getId(),
                    SiddhiConstants.METRIC_INFIX_TABLES, SiddhiConstants.METRIC_TYPE_UPDATE);
            latencyTrackerDelete = QueryParserHelper.createLatencyTracker(siddhiAppContext, tableDefinition.getId(),
                    SiddhiConstants.METRIC_INFIX_TABLES, SiddhiConstants.METRIC_TYPE_DELETE);
            latencyTrackerUpdateOrInsert = QueryParserHelper.createLatencyTracker(siddhiAppContext,
                    tableDefinition.getId(), SiddhiConstants.METRIC_INFIX_TABLES,
                    SiddhiConstants.METRIC_TYPE_UPDATE_OR_INSERT);
            latencyTrackerContains = QueryParserHelper.createLatencyTracker(siddhiAppContext, tableDefinition.getId(),
                    SiddhiConstants.METRIC_INFIX_TABLES, SiddhiConstants.METRIC_TYPE_CONTAINS);

            throughputTrackerFind = QueryParserHelper.createThroughputTracker(siddhiAppContext, tableDefinition.getId(),
                    SiddhiConstants.METRIC_INFIX_TABLES, SiddhiConstants.METRIC_TYPE_FIND);
            throughputTrackerInsert = QueryParserHelper.createThroughputTracker(siddhiAppContext,
                    tableDefinition.getId(), SiddhiConstants.METRIC_INFIX_TABLES, SiddhiConstants.METRIC_TYPE_INSERT);
            throughputTrackerUpdate = QueryParserHelper.createThroughputTracker(siddhiAppContext,
                    tableDefinition.getId(), SiddhiConstants.METRIC_INFIX_TABLES, SiddhiConstants.METRIC_TYPE_UPDATE);
            throughputTrackerDelete = QueryParserHelper.createThroughputTracker(siddhiAppContext,
                    tableDefinition.getId(), SiddhiConstants.METRIC_INFIX_TABLES, SiddhiConstants.METRIC_TYPE_DELETE);
            throughputTrackerUpdateOrInsert = QueryParserHelper.createThroughputTracker(siddhiAppContext,
                    tableDefinition.getId(), SiddhiConstants.METRIC_INFIX_TABLES,
                    SiddhiConstants.METRIC_TYPE_UPDATE_OR_INSERT);
            throughputTrackerContains = QueryParserHelper.createThroughputTracker(siddhiAppContext,
                    tableDefinition.getId(), SiddhiConstants.METRIC_INFIX_TABLES, SiddhiConstants.METRIC_TYPE_CONTAINS);

        }
        init(tableDefinition, storeEventPool, storeEventCloner, configReader, siddhiAppContext, recordTableHandler);
    }

    protected abstract void init(TableDefinition tableDefinition, StreamEventFactory storeEventPool,
                                 StreamEventCloner storeEventCloner, ConfigReader configReader,
                                 SiddhiAppContext siddhiAppContext, RecordTableHandler recordTableHandler);

    public TableDefinition getTableDefinition() {
        return tableDefinition;
    }

    private boolean isObjectColumnPresent(TableDefinition tableDefinition) {
        return tableDefinition.getAttributeList()
                .stream().anyMatch(attribute -> attribute.getType() == Attribute.Type.OBJECT);
    }

    protected void onAddError(ComplexEventChunk<StreamEvent> addingEventChunk, Exception e) {
        OnErrorAction errorAction = onErrorAction;
        if (e instanceof ConnectionUnavailableException) {
            isConnected.set(false);
            if (errorAction == OnErrorAction.STORE) {
                handleStoreAddError(addingEventChunk, true, e);
                LOG.error("Error on '" + siddhiAppContext.getName() + "' while performing add for events  at '"
                        + tableDefinition.getId() + "'. Events saved '" + addingEventChunk.toString() + "'");
                if (LOG.isDebugEnabled()) {
                    LOG.debug(e);
                }
                if (!isTryingToConnect.get()) {
                    connectWithRetry();
                }
            } else {
                if (isTryingToConnect.get()) {
                    LOG.warn("Error on '" + siddhiAppContext.getName() + "' while performing add for events '" +
                            addingEventChunk + "', operation busy waiting at Table '"
                            + tableDefinition.getId() + "' as its trying to reconnect!");
                    waitWhileConnect();
                    LOG.info("SiddhiApp '" + siddhiAppContext.getName() + "' table '" + tableDefinition.getId()
                            + "' has become available for add operation for events '" + addingEventChunk + "'");
                    add(addingEventChunk);
                } else {
                    connectWithRetry();
                    add(addingEventChunk);
                }
            }
        } else if (e instanceof DatabaseRuntimeException) {
            if (errorAction == OnErrorAction.STORE) {
                handleStoreAddError(addingEventChunk, false, e);
                LOG.error("Error on '" + siddhiAppContext.getName() + "' while performing add for events  at '"
                        + tableDefinition.getId() + "'. Events saved '" + addingEventChunk.toString() + "'");
                if (LOG.isDebugEnabled()) {
                    LOG.debug(e);
                }
            } else {
                throw (DatabaseRuntimeException) e;
            }
        }
    }

    private void handleStoreAddError(ComplexEventChunk addingEventChunk, boolean isFromConnectionUnavailableException,
                                     Exception e) {
        addingEventChunk.reset();
        ReplayableTableRecord record = new ReplayableTableRecord(addingEventChunk);
        record.setFromConnectionUnavailableException(isFromConnectionUnavailableException);
        record.setEditable(!isObjectColumnPresent);
        ErroneousEvent erroneousEvent = new ErroneousEvent(record, e, e.getMessage());
        erroneousEvent.setOriginalPayload(ErrorHandlerUtils.constructAddErrorRecordString(addingEventChunk,
                isFromConnectionUnavailableException, tableDefinition, e));
        ErrorStoreHelper.storeErroneousEvent(siddhiAppContext.getSiddhiContext().getErrorStore(),
                ErrorOccurrence.STORE_ON_TABLE_ADD, siddhiAppContext.getName(), erroneousEvent,
                tableDefinition.getId());
    }

    public void addEvents(ComplexEventChunk<StreamEvent> addingEventChunk, int noOfEvents) {
        if (latencyTrackerInsert != null &&
                Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
            latencyTrackerInsert.markIn();
        }
        addingEventChunk.reset();
        add(addingEventChunk);
        if (throughputTrackerInsert != null &&
                Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
            throughputTrackerInsert.eventsIn(noOfEvents);
        }
    }

    public abstract void add(ComplexEventChunk<StreamEvent> addingEventChunk);

    public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        if (isConnected.get()) {
            try {
                if (latencyTrackerFind != null &&
                        Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
                    latencyTrackerFind.markIn();
                }
                StreamEvent results = find(compiledCondition, matchingEvent);
                if (throughputTrackerFind != null &&
                        Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
                    throughputTrackerFind.eventIn();
                }
                return results;
            } catch (ConnectionUnavailableException e) {
                isConnected.set(false);
                LOG.error(ExceptionUtil.getMessageWithContext(e, siddhiAppContext) +
                        " Connection unavailable at Table '" + tableDefinition.getId() +
                        "', will retry connection immediately.", e);
                connectWithRetry();
                return find(matchingEvent, compiledCondition);
            } finally {
                if (latencyTrackerFind != null &&
                        Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
                    latencyTrackerFind.markOut();
                }
            }
        } else if (isTryingToConnect.get()) {
            LOG.warn("Error on '" + siddhiAppContext.getName() + "' while performing find for events '" +
                    matchingEvent + "', operation busy waiting at Table '" + tableDefinition.getId() +
                    "' as its trying to reconnect!");
            waitWhileConnect();
            LOG.info("SiddhiApp '" + siddhiAppContext.getName() + "' table '" + tableDefinition.getId() +
                    "' has become available for find operation for events '" + matchingEvent + "'");
            return find(matchingEvent, compiledCondition);
        } else {
            connectWithRetry();
            return find(matchingEvent, compiledCondition);
        }
    }

    protected abstract StreamEvent find(CompiledCondition compiledCondition, StateEvent matchingEvent)
            throws ConnectionUnavailableException;

    protected void onDeleteError(ComplexEventChunk<StateEvent> deletingEventChunk, CompiledCondition compiledCondition,
                                 Exception e) {
        OnErrorAction errorAction = onErrorAction;
        if (e instanceof ConnectionUnavailableException) {
            isConnected.set(false);
            if (errorAction == OnErrorAction.STORE) {
                deletingEventChunk.reset();
                ErroneousEvent erroneousEvent = new ErroneousEvent(
                        new ReplayableTableRecord(deletingEventChunk, compiledCondition), e, e.getMessage());
                erroneousEvent.setOriginalPayload(ErrorHandlerUtils.constructErrorRecordString(deletingEventChunk,
                        isObjectColumnPresent, tableDefinition, e));
                ErrorStoreHelper.storeErroneousEvent(siddhiAppContext.getSiddhiContext().getErrorStore(),
                        ErrorOccurrence.STORE_ON_TABLE_DELETE, siddhiAppContext.getName(), erroneousEvent,
                        tableDefinition.getId());
                LOG.error("Error on '" + siddhiAppContext.getName() + "' while performing delete for events  at '"
                        + tableDefinition.getId() + "'. Events saved '" + deletingEventChunk.toString() + "'");
                if (LOG.isDebugEnabled()) {
                    LOG.debug(e);
                }
                if (!isTryingToConnect.get()) {
                    connectWithRetry();
                }
            } else {
                if (isTryingToConnect.get()) {
                    LOG.warn("Error on '" + siddhiAppContext.getName() + "' while performing delete for events '" +
                            deletingEventChunk + "', operation busy waiting at Table '" + tableDefinition.getId() +
                            "' as its trying to reconnect!");
                    waitWhileConnect();
                    LOG.info("SiddhiApp '" + siddhiAppContext.getName() + "' table '" + tableDefinition.getId() +
                            "' has become available for delete operation for events '" + deletingEventChunk + "'");
                    delete(deletingEventChunk, compiledCondition);
                } else {
                    connectWithRetry();
                    delete(deletingEventChunk, compiledCondition);
                }
            }
        } else if (e instanceof DatabaseRuntimeException) {
            if (errorAction == OnErrorAction.STORE) {
                deletingEventChunk.reset();
                ReplayableTableRecord record = new ReplayableTableRecord(deletingEventChunk, compiledCondition);
                record.setFromConnectionUnavailableException(false);
                ErroneousEvent erroneousEvent = new ErroneousEvent(record, e, e.getMessage());
                erroneousEvent.setOriginalPayload(ErrorHandlerUtils.constructErrorRecordString(deletingEventChunk,
                        isObjectColumnPresent, tableDefinition, e));
                ErrorStoreHelper.storeErroneousEvent(siddhiAppContext.getSiddhiContext().getErrorStore(),
                        ErrorOccurrence.STORE_ON_TABLE_DELETE, siddhiAppContext.getName(), erroneousEvent,
                        tableDefinition.getId());
                LOG.error("Error on '" + siddhiAppContext.getName() + "' while performing delete for events  at '"
                        + tableDefinition.getId() + "'. Events saved '" + deletingEventChunk.toString() + "'");
                if (LOG.isDebugEnabled()) {
                    LOG.debug(e);
                }
            }
        }
    }

    public void deleteEvents(ComplexEventChunk<StateEvent> deletingEventChunk, CompiledCondition compiledCondition,
                             int noOfEvents) {
        if (latencyTrackerDelete != null &&
                Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
            latencyTrackerDelete.markIn();
        }
        delete(deletingEventChunk, compiledCondition);
        if (throughputTrackerDelete != null &&
                Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
            throughputTrackerDelete.eventsIn(noOfEvents);
        }
    }

    public abstract void delete(ComplexEventChunk<StateEvent> deletingEventChunk,
                                CompiledCondition compiledCondition);

    protected void onUpdateError(ComplexEventChunk<StateEvent> updatingEventChunk, CompiledCondition compiledCondition,
                                 CompiledUpdateSet compiledUpdateSet, Exception e) {
        OnErrorAction errorAction = onErrorAction;
        if (e instanceof ConnectionUnavailableException) {
            isConnected.set(false);
            if (errorAction == OnErrorAction.STORE) {
                updatingEventChunk.reset();
                ErroneousEvent erroneousEvent = new ErroneousEvent(
                        new ReplayableTableRecord(updatingEventChunk, compiledCondition, compiledUpdateSet), e,
                        e.getMessage());
                erroneousEvent.setOriginalPayload(ErrorHandlerUtils.constructErrorRecordString(updatingEventChunk,
                        isObjectColumnPresent, tableDefinition, e));
                ErrorStoreHelper.storeErroneousEvent(siddhiAppContext.getSiddhiContext().getErrorStore(),
                        ErrorOccurrence.STORE_ON_TABLE_UPDATE, siddhiAppContext.getName(), erroneousEvent,
                        tableDefinition.getId());
                LOG.error("Error on '" + siddhiAppContext.getName() + "' while performing update for events  " +
                        "at '" + tableDefinition.getId() + "'. Events saved '" + updatingEventChunk.toString() +
                        "'");
                if (LOG.isDebugEnabled()) {
                    LOG.debug(e);
                }
                if (!isTryingToConnect.get()) {
                    connectWithRetry();
                }
            } else {
                if (isTryingToConnect.get()) {
                    LOG.warn("Error on '" + siddhiAppContext.getName() + "' while performing update for " +
                            "events '" + updatingEventChunk + "', operation busy waiting at Table '" +
                            tableDefinition.getId() + "' as its trying to reconnect!");
                    waitWhileConnect();
                    LOG.info("SiddhiApp '" + siddhiAppContext.getName() + "' table '" + tableDefinition.getId()
                            + "' has become available for update operation for events '" + updatingEventChunk
                            + "'");
                    update(updatingEventChunk, compiledCondition, compiledUpdateSet);
                } else {
                    connectWithRetry();
                    update(updatingEventChunk, compiledCondition, compiledUpdateSet);
                }
            }
        } else if (e instanceof DatabaseRuntimeException) {
            if (errorAction == OnErrorAction.STORE) {
                updatingEventChunk.reset();
                ReplayableTableRecord record = new ReplayableTableRecord(updatingEventChunk, compiledCondition,
                        compiledUpdateSet);
                record.setFromConnectionUnavailableException(false);
                ErroneousEvent erroneousEvent = new ErroneousEvent(record, e, e.getMessage());
                erroneousEvent.setOriginalPayload(ErrorHandlerUtils.constructErrorRecordString(updatingEventChunk,
                        isObjectColumnPresent, tableDefinition, e));
                ErrorStoreHelper.storeErroneousEvent(siddhiAppContext.getSiddhiContext().getErrorStore(),
                        ErrorOccurrence.STORE_ON_TABLE_UPDATE, siddhiAppContext.getName(), erroneousEvent,
                        tableDefinition.getId());
                LOG.error("Error on '" + siddhiAppContext.getName() + "' while performing update for events  " +
                        "at '" + tableDefinition.getId() + "'. Events saved '" + updatingEventChunk.toString() +
                        "'");
                if (LOG.isDebugEnabled()) {
                    LOG.debug(e);
                }
            }
        }
    }


    public void updateEvents(ComplexEventChunk<StateEvent> updatingEventChunk,
                             CompiledCondition compiledCondition,
                             CompiledUpdateSet compiledUpdateSet, int noOfEvents) {
        if (latencyTrackerUpdate != null &&
                Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
            latencyTrackerUpdate.markIn();
        }
        update(updatingEventChunk, compiledCondition, compiledUpdateSet);
        if (throughputTrackerUpdate != null &&
                Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
            throughputTrackerUpdate.eventsIn(noOfEvents);
        }
    }

    public abstract void update(ComplexEventChunk<StateEvent> updatingEventChunk,
                                CompiledCondition compiledCondition,
                                CompiledUpdateSet compiledUpdateSet);

    protected void onUpdateOrAddError(ComplexEventChunk<StateEvent> updateOrAddingEventChunk,
                                      CompiledCondition compiledCondition, CompiledUpdateSet compiledUpdateSet,
                                      AddingStreamEventExtractor addingStreamEventExtractor,
                                      Exception e) {
        OnErrorAction errorAction = onErrorAction;
        if (e instanceof ConnectionUnavailableException) {
            isConnected.set(false);
            if (errorAction == OnErrorAction.STORE) {
                updateOrAddingEventChunk.reset();
                ErroneousEvent erroneousEvent = new ErroneousEvent(
                        new ReplayableTableRecord(updateOrAddingEventChunk, compiledCondition, compiledUpdateSet,
                                addingStreamEventExtractor), e, e.getMessage());
                erroneousEvent.setOriginalPayload(ErrorHandlerUtils.constructErrorRecordString(updateOrAddingEventChunk,
                        isObjectColumnPresent, tableDefinition, e));
                ErrorStoreHelper.storeErroneousEvent(siddhiAppContext.getSiddhiContext().getErrorStore(),
                        ErrorOccurrence.STORE_ON_TABLE_UPDATE_OR_ADD, siddhiAppContext.getName(), erroneousEvent,
                        tableDefinition.getId());
                LOG.error("Error on '" + siddhiAppContext.getName() + "' while performing update or add for " +
                        "events  at '" + tableDefinition.getId() + "'. Events saved '" +
                        updateOrAddingEventChunk.toString() + "'");
                if (LOG.isDebugEnabled()) {
                    LOG.debug(e);
                }
                if (!isTryingToConnect.get()) {
                    connectWithRetry();
                }
            } else {
                if (isTryingToConnect.get()) {
                    LOG.warn("Error on '" + siddhiAppContext.getName() + "' while performing update or add for " +
                            "events '" + updateOrAddingEventChunk + "', operation busy waiting at Table '" +
                            tableDefinition.getId() + "' as its trying to reconnect!");
                    waitWhileConnect();
                    LOG.info("SiddhiApp '" + siddhiAppContext.getName() + "' table '" + tableDefinition.getId() +
                            "' has become available for update or add operation for events '" +
                            updateOrAddingEventChunk + "'");
                    updateOrAdd(updateOrAddingEventChunk, compiledCondition, compiledUpdateSet,
                            addingStreamEventExtractor);
                } else {
                    connectWithRetry();
                    updateOrAdd(updateOrAddingEventChunk, compiledCondition, compiledUpdateSet,
                            addingStreamEventExtractor);
                }
            }
        } else if (e instanceof DatabaseRuntimeException) {
            if (errorAction == OnErrorAction.STORE) {
                updateOrAddingEventChunk.reset();
                ReplayableTableRecord record = new ReplayableTableRecord(updateOrAddingEventChunk,
                        compiledCondition, compiledUpdateSet, addingStreamEventExtractor);
                record.setFromConnectionUnavailableException(false);
                ErroneousEvent erroneousEvent = new ErroneousEvent(record, e, e.getMessage());
                erroneousEvent.setOriginalPayload(ErrorHandlerUtils.constructErrorRecordString(updateOrAddingEventChunk,
                        isObjectColumnPresent, tableDefinition, e));
                ErrorStoreHelper.storeErroneousEvent(siddhiAppContext.getSiddhiContext().getErrorStore(),
                        ErrorOccurrence.STORE_ON_TABLE_UPDATE_OR_ADD, siddhiAppContext.getName(), erroneousEvent,
                        tableDefinition.getId());
                LOG.error("Error on '" + siddhiAppContext.getName() + "' while performing update or add for " +
                        "events  at '" + tableDefinition.getId() + "'. Events saved '" +
                        updateOrAddingEventChunk.toString() + "'");
                if (LOG.isDebugEnabled()) {
                    LOG.debug(e);
                }
            }
        }
    }

    public void updateOrAddEvents(ComplexEventChunk<StateEvent> updateOrAddingEventChunk,
                                  CompiledCondition compiledCondition,
                                  CompiledUpdateSet compiledUpdateSet,
                                  AddingStreamEventExtractor addingStreamEventExtractor, int noOfEvents) {
        if (latencyTrackerUpdateOrInsert != null &&
                Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
            latencyTrackerUpdateOrInsert.markIn();
        }
        updateOrAdd(updateOrAddingEventChunk, compiledCondition, compiledUpdateSet,
                addingStreamEventExtractor);
        if (throughputTrackerUpdateOrInsert != null &&
                Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
            throughputTrackerUpdateOrInsert.eventsIn(noOfEvents);
        }
    }

    public abstract void updateOrAdd(ComplexEventChunk<StateEvent> updateOrAddingEventChunk,
                                     CompiledCondition compiledCondition,
                                     CompiledUpdateSet compiledUpdateSet,
                                     AddingStreamEventExtractor addingStreamEventExtractor);

    public boolean containsEvent(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        if (isConnected.get()) {
            try {
                if (latencyTrackerContains != null &&
                        Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
                    latencyTrackerContains.markIn();
                }
                boolean results = contains(matchingEvent, compiledCondition);
                if (throughputTrackerContains != null &&
                        Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
                    throughputTrackerContains.eventIn();
                }
                return results;
            } catch (ConnectionUnavailableException e) {
                isConnected.set(false);
                LOG.error(ExceptionUtil.getMessageWithContext(e, siddhiAppContext) +
                        " Connection unavailable at Table '" + tableDefinition.getId() +
                        "', will retry connection immediately.", e);
                connectWithRetry();
                return containsEvent(matchingEvent, compiledCondition);
            } finally {
                if (latencyTrackerContains != null &&
                        Level.BASIC.compareTo(siddhiAppContext.getRootMetricsLevel()) <= 0) {
                    latencyTrackerContains.markOut();
                }
            }
        } else if (isTryingToConnect.get()) {
            LOG.warn("Error on '" + siddhiAppContext.getName() + "' while performing contains check for event '" +
                    matchingEvent + "', operation busy waiting at Table '" + tableDefinition.getId() +
                    "' as its trying to reconnect!");
            waitWhileConnect();
            LOG.info("SiddhiApp '" + siddhiAppContext.getName() + "' table '" + tableDefinition.getId() +
                    "' has become available for contains check operation for matching event '" + matchingEvent + "'");
            return containsEvent(matchingEvent, compiledCondition);
        } else {
            connectWithRetry();
            return containsEvent(matchingEvent, compiledCondition);
        }
    }

    protected abstract boolean contains(StateEvent matchingEvent, CompiledCondition compiledCondition)
            throws ConnectionUnavailableException;

    public void connectWithRetry() {
        if (!isConnected.get()) {
            isTryingToConnect.set(true);
            try {
                connectAndLoadCache();
                isConnected.set(true);
                synchronized (this) {
                    isTryingToConnect.set(false);
                    this.notifyAll();
                }
                backoffRetryCounter.reset();
            } catch (ConnectionUnavailableException e) {
                LOG.error(StringUtil.removeCRLFCharacters(ExceptionUtil.getMessageWithContext(e,
                        siddhiAppContext)) + " Error while connecting to Table '" +
                        StringUtil.removeCRLFCharacters(tableDefinition.getId())
                        + "', will retry in '" + StringUtil.removeCRLFCharacters(
                        backoffRetryCounter.getTimeInterval()) + "'.", e);
                scheduledExecutorService.schedule(new Runnable() {
                    @Override
                    public void run() {
                        connectWithRetry();
                    }
                }, backoffRetryCounter.getTimeIntervalMillis(), TimeUnit.MILLISECONDS);
                backoffRetryCounter.increment();
            } catch (RuntimeException e) {
                LOG.error(StringUtil.removeCRLFCharacters(ExceptionUtil.getMessageWithContext(e,
                        siddhiAppContext)) + " . Error while connecting to Table '" +
                        StringUtil.removeCRLFCharacters(tableDefinition.getId()) + "'.", e);
                throw e;
            }
        }
    }

    public void setIsConnectedToFalse() {
        this.isConnected.set(false);
    }

    public boolean getIsTryingToConnect() {
        return isTryingToConnect.get();
    }

    public boolean getIsConnected() {
        return isConnected.get();
    }

    /**
     * Busy wait the threads which bind to this object and control the execution flow
     * until table connection recovered.
     */
    public void waitWhileConnect() {
        try {
            synchronized (this) {
                while (isTryingToConnect.get()) {
                    this.wait();
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Error on SiddhiApp '" + siddhiAppContext.getName() + "', interrupted while " +
                    "busy wait on connection retrying condition " + e.getMessage(), e);
        }
    }

    /**
     * Builds the "compiled" set clause of an update query.
     * Here, all the pre-processing that can be done prior to receiving the update event is done,
     * so that such pre-processing work will not be done at each update-event-arrival.
     *
     * @param updateSet                   the set of assignment expressions, each containing the table column to be
     *                                    updated and the expression to be assigned.
     * @param matchingMetaInfoHolder      the meta structure of the incoming matchingEvent
     * @param variableExpressionExecutors the list of variable ExpressionExecutors already created
     * @param tableMap                    map of event tables
     * @param siddhiQueryContext          current siddhi query context
     * @return CompiledUpdateSet
     */
    public abstract CompiledUpdateSet compileUpdateSet(UpdateSet updateSet,
                                                       MatchingMetaInfoHolder matchingMetaInfoHolder,
                                                       List<VariableExpressionExecutor> variableExpressionExecutors,
                                                       Map<String, Table> tableMap,
                                                       SiddhiQueryContext siddhiQueryContext);

    protected abstract void connectAndLoadCache() throws ConnectionUnavailableException;

    protected abstract void disconnect();

    protected abstract void destroy();

    public RecordTableHandler getHandler() {
        return recordTableHandler;
    }

    public void shutdown() {
        disconnect();
        destroy();
        isConnected.set(false);
        isTryingToConnect.set(false);
    }

    public abstract boolean isStateful();

    /**
     * Different Type of On Error Actions
     */
    public enum OnErrorAction {
        LOG,
        STORE,
        RETRY
    }
}
