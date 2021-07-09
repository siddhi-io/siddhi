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

package io.siddhi.core;

import com.lmax.disruptor.ExceptionHandler;
import io.siddhi.core.debugger.SiddhiDebugger;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.CannotClearSiddhiAppStateException;
import io.siddhi.core.exception.CannotRestoreSiddhiAppStateException;
import io.siddhi.core.partition.PartitionRuntime;
import io.siddhi.core.query.QueryRuntime;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.input.TableInputHandler;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.stream.output.sink.Sink;
import io.siddhi.core.table.Table;
import io.siddhi.core.trigger.Trigger;
import io.siddhi.core.util.snapshot.PersistenceReference;
import io.siddhi.core.util.statistics.metrics.Level;
import io.siddhi.core.window.Window;
import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.AggregationDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.definition.WindowDefinition;
import io.siddhi.query.api.execution.query.OnDemandQuery;
import io.siddhi.query.api.execution.query.StoreQuery;

import java.beans.ExceptionListener;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Keep streamDefinitions, partitionRuntimes, queryRuntimes of an SiddhiApp and streamJunctions and inputHandlers used.
 */
public interface SiddhiAppRuntime {


    String getName();

    SiddhiApp getSiddhiApp();

    /**
     * Get the stream definition map.
     *
     * @return Map of {@link StreamDefinition}s.
     */
    Map<String, StreamDefinition> getStreamDefinitionMap();

    /**
     * Get the table definition map.
     *
     * @return Map of {@link TableDefinition}s.
     */
    Map<String, TableDefinition> getTableDefinitionMap();

    /**
     * Get the window definition map.
     *
     * @return Map of {@link WindowDefinition}s.
     */
    Map<String, WindowDefinition> getWindowDefinitionMap();

    /**
     * Get the aggregation definition map.
     *
     * @return Map of {@link AggregationDefinition}s.
     */
    Map<String, AggregationDefinition> getAggregationDefinitionMap();

    /**
     * Get the names of the available queries.
     *
     * @return string set of query names.
     */
    Set<String> getQueryNames();

    Map<String, Map<String, AbstractDefinition>> getPartitionedInnerStreamDefinitionMap();

    Collection<List<Source>> getSources();

    Collection<List<Sink>> getSinks();

    Collection<Table> getTables();

    Collection<Window> getWindows();

    Collection<Trigger> getTiggers();

    Collection<QueryRuntime> getQueries();

    Collection<PartitionRuntime> getPartitions();

    void addCallback(String streamId, StreamCallback streamCallback);

    void addCallback(String queryName, QueryCallback callback);

    void removeCallback(StreamCallback streamCallback);

    void removeCallback(QueryCallback streamCallback);

    Event[] query(String onDemandQuery);

    @Deprecated
    Event[] query(StoreQuery storeQuery);

    Event[] query(OnDemandQuery storeQuery);

    @Deprecated
    Attribute[] getStoreQueryOutputAttributes(String storeQuery);

    @Deprecated
    Attribute[] getStoreQueryOutputAttributes(StoreQuery storeQuery);

    Attribute[] getOnDemandQueryOutputAttributes(String onDemandQuery);

    Attribute[] getOnDemandQueryOutputAttributes(OnDemandQuery onDemandQuery);

    InputHandler getInputHandler(String streamId);

    TableInputHandler getTableInputHandler(String tableId);

    void setPurgingEnabled(boolean purgingEnabled);

    void start();

    void startWithoutSources();

    void startSources();

    void shutdown();

    SiddhiDebugger debug();

    PersistenceReference persist();

    byte[] snapshot();

    void restore(byte[] snapshot) throws CannotRestoreSiddhiAppStateException;

    void restoreRevision(String revision) throws CannotRestoreSiddhiAppStateException;

    String restoreLastRevision() throws CannotRestoreSiddhiAppStateException;

    void clearAllRevisions() throws CannotClearSiddhiAppStateException;

    void handleExceptionWith(ExceptionHandler<Object> exceptionHandler);

    void handleRuntimeExceptionWith(ExceptionListener exceptionListener);

    /**
     * Method to check the Siddhi App statistics level enabled.
     *
     * @return Level value of Siddhi App statistics state
     */
    Level getStatisticsLevel();

    /**
     * To enable, disable and change Siddhi App statistics level on runtime.
     *
     * @param level whether statistics is OFF, BASIC or DETAIL
     */
    void setStatisticsLevel(Level level);

    /**
     * To enable and disable Siddhi App playback mode on runtime along with optional parameters.
     *
     * @param playBackEnabled         whether playback is enabled or not
     * @param idleTime
     * @param incrementInMilliseconds
     */
    void enablePlayBack(boolean playBackEnabled, Long idleTime, Long incrementInMilliseconds);

    /**
     * Method to get Siddhi App runtime warnings.
     *
     * @return list of recorded runtime warnings.
     */
    Set<String> getWarnings();
}
