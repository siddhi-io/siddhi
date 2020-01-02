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

package io.siddhi.core.config;

import com.lmax.disruptor.ExceptionHandler;
import io.siddhi.core.function.Script;
import io.siddhi.core.trigger.Trigger;
import io.siddhi.core.util.IdGenerator;
import io.siddhi.core.util.Scheduler;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.ThreadBarrier;
import io.siddhi.core.util.extension.holder.ExternalReferencedHolder;
import io.siddhi.core.util.snapshot.SnapshotService;
import io.siddhi.core.util.snapshot.state.EmptyStateHolder;
import io.siddhi.core.util.snapshot.state.SingleStateHolder;
import io.siddhi.core.util.snapshot.state.SingleSyncStateHolder;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.snapshot.state.StateHolder;
import io.siddhi.core.util.statistics.StatisticsManager;
import io.siddhi.core.util.statistics.metrics.Level;
import io.siddhi.core.util.timestamp.TimestampGenerator;
import io.siddhi.query.api.SiddhiApp;

import java.beans.ExceptionListener;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Holder object for context information of {@link SiddhiApp}.
 */
public class SiddhiAppContext {

    private static final ThreadLocal<String> GROUP_BY_KEY = new ThreadLocal<>();
    private static final ThreadLocal<String> PARTITION_KEY = new ThreadLocal<>();
    private SiddhiContext siddhiContext = null;
    private String name;
    private boolean playback;
    private boolean enforceOrder;
    private Level rootMetricsLevel;
    private StatisticsManager statisticsManager = null;
    private ExecutorService executorService;
    private ScheduledExecutorService scheduledExecutorService;
    private List<ExternalReferencedHolder> externalReferencedHolders;
    private List<Trigger> triggerHolders;
    private SnapshotService snapshotService;
    private ThreadBarrier threadBarrier = null;
    private TimestampGenerator timestampGenerator = null;
    private IdGenerator idGenerator;
    private Map<String, Script> scriptFunctionMap;
    private ExceptionHandler<Object> disruptorExceptionHandler;
    private ExceptionListener runtimeExceptionListener;
    private int bufferSize;
    private String siddhiAppString;
    private List<String> includedMetrics;
    private boolean transportChannelCreationEnabled;
    private List<Scheduler> schedulerList;
    private SiddhiApp siddhiApp;

    public SiddhiAppContext() {
        this.externalReferencedHolders = Collections.synchronizedList(new LinkedList<>());
        this.triggerHolders = Collections.synchronizedList(new LinkedList<>());
        this.scriptFunctionMap = new HashMap<String, Script>();
        this.schedulerList = new ArrayList<Scheduler>();
        this.rootMetricsLevel = Level.OFF;
    }

    public static void startGroupByFlow(String key) {
        GROUP_BY_KEY.set(key);
    }

    public static void stopGroupByFlow() {
        GROUP_BY_KEY.set(null);
    }

    public static void startPartitionFlow(String key) {
        PARTITION_KEY.set(key);
    }

    public static void stopPartitionFlow() {
        PARTITION_KEY.set(null);
    }

    public static String getCurrentFlowId() {
        return PARTITION_KEY.get() + "--" + GROUP_BY_KEY.get();
    }

    public static String getPartitionFlowId() {
        return PARTITION_KEY.get();
    }

    public static String getGroupByFlowId() {
        return GROUP_BY_KEY.get();
    }

    public SiddhiContext getSiddhiContext() {
        return siddhiContext;
    }

    public void setSiddhiContext(SiddhiContext siddhiContext) {
        this.siddhiContext = siddhiContext;
    }

    /**
     * Attributes that are common across all the Siddhi Apps
     *
     * @return Attribute Map&lt;String, Object&gt;
     */
    public Map<String, Object> getAttributes() {
        return siddhiContext.getAttributes();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isPlayback() {
        return playback;
    }

    public void setPlayback(boolean playback) {
        this.playback = playback;
    }

    public boolean isEnforceOrder() {
        return enforceOrder;
    }

    public void setEnforceOrder(boolean enforceOrder) {
        this.enforceOrder = enforceOrder;
    }

    public Level getRootMetricsLevel() {
        return rootMetricsLevel;
    }

    public void setRootMetricsLevel(Level rootMetricsLevel) {
        this.rootMetricsLevel = rootMetricsLevel;
    }

    public StatisticsManager getStatisticsManager() {
        return statisticsManager;
    }

    public void setStatisticsManager(StatisticsManager statisticsManager) {
        this.statisticsManager = statisticsManager;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public void setScheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
    }

    public void addEternalReferencedHolder(ExternalReferencedHolder externalReferencedHolder) {
        externalReferencedHolders.add(externalReferencedHolder);
    }

    public List<ExternalReferencedHolder> getExternalReferencedHolders() {
        return Collections.unmodifiableList(new ArrayList<>(externalReferencedHolders));
    }

    public List<Trigger> getTriggerHolders() {
        return Collections.unmodifiableList(new ArrayList<>(triggerHolders));
    }

    public void addTrigger(Trigger trigger) {
        triggerHolders.add(trigger);
    }

    public ThreadBarrier getThreadBarrier() {
        return threadBarrier;
    }

    public void setThreadBarrier(ThreadBarrier threadBarrier) {
        this.threadBarrier = threadBarrier;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public TimestampGenerator getTimestampGenerator() {
        return timestampGenerator;
    }

    public void setTimestampGenerator(TimestampGenerator timestampGenerator) {
        this.timestampGenerator = timestampGenerator;
    }

    public SnapshotService getSnapshotService() {
        return snapshotService;
    }

    public void setSnapshotService(SnapshotService snapshotService) {
        this.snapshotService = snapshotService;
    }

//    public IdGenerator getElementIdGenerator() {
//        return idGenerator;
//    }

    public void setIdGenerator(IdGenerator idGenerator) {
        this.idGenerator = idGenerator;
    }

    public Script getScript(String name) {
        return scriptFunctionMap.get(name);
    }

    public boolean isFunctionExist(String name) {
        return scriptFunctionMap.get(name) != null;
    }

    public Map<String, Script> getScriptFunctionMap() {
        return scriptFunctionMap;
    }

    public ExceptionHandler<Object> getDisruptorExceptionHandler() {
        if (disruptorExceptionHandler != null) {
            return disruptorExceptionHandler;
        } else {
            return siddhiContext.getDefaultDisrupterExceptionHandler();
        }
    }

    public void setDisruptorExceptionHandler(ExceptionHandler<Object> disruptorExceptionHandler) {
        this.disruptorExceptionHandler = disruptorExceptionHandler;
    }

    public ExceptionListener getRuntimeExceptionListener() {
        return runtimeExceptionListener;
    }

    public void setRuntimeExceptionListener(ExceptionListener runtimeExceptionListener) {
        this.runtimeExceptionListener = runtimeExceptionListener;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public String getSiddhiAppString() {
        return siddhiAppString;
    }

    public void setSiddhiAppString(String siddhiAppString) {
        this.siddhiAppString = siddhiAppString;
    }

    public SiddhiApp getSiddhiApp() {
        return siddhiApp;
    }

    public void setSiddhiApp(SiddhiApp siddhiApp) {
        this.siddhiApp = siddhiApp;
    }

    public List<String> getIncludedMetrics() {
        return includedMetrics;
    }

    public void setIncludedMetrics(List<String> includedMetrics) {
        this.includedMetrics = includedMetrics;
    }

    public boolean isTransportChannelCreationEnabled() {
        return transportChannelCreationEnabled;
    }

    public void setTransportChannelCreationEnabled(boolean transportChannelCreationEnabled) {
        this.transportChannelCreationEnabled = transportChannelCreationEnabled;
    }

    public void addScheduler(Scheduler scheduler) {
        this.schedulerList.add(scheduler);
    }

    public List<Scheduler> getSchedulerList() {
        return schedulerList;
    }

    public StateHolder generateStateHolder(String name, StateFactory stateFactory) {
        return generateStateHolder(name, stateFactory, false);
    }

    public StateHolder generateStateHolder(String name, StateFactory stateFactory, boolean unSafe) {
        if (stateFactory != null) {
            StateHolder stateHolder;
            if (unSafe) {
                stateHolder = new SingleStateHolder(stateFactory);
            } else {
                stateHolder = new SingleSyncStateHolder(stateFactory);
            }
            if (SnapshotService.getSkipStateStorageThreadLocal().get() == null ||
                    !SnapshotService.getSkipStateStorageThreadLocal().get()) {
                Map<String, StateHolder> stateHolderMap = getSnapshotService().getStateHolderMap(
                        SiddhiConstants.PARTITION_ID_DEFAULT, SiddhiConstants.PARTITION_ID_DEFAULT);
                stateHolderMap.put(name + "-" + idGenerator.createNewId(), stateHolder);
            }
            return stateHolder;
        } else {
            return new EmptyStateHolder();
        }
    }

}
