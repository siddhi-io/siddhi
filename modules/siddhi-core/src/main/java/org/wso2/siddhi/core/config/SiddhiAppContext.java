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

package org.wso2.siddhi.core.config;

import com.lmax.disruptor.ExceptionHandler;
import org.wso2.siddhi.core.function.Script;
import org.wso2.siddhi.core.stream.StreamJunction;
import org.wso2.siddhi.core.util.ElementIdGenerator;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.core.util.ThreadBarrier;
import org.wso2.siddhi.core.util.extension.holder.EternalReferencedHolder;
import org.wso2.siddhi.core.util.snapshot.SnapshotService;
import org.wso2.siddhi.core.util.statistics.StatisticsManager;
import org.wso2.siddhi.core.util.timestamp.TimestampGenerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Holder object for context information of {@link org.wso2.siddhi.query.api.SiddhiApp}.
 */
public class SiddhiAppContext {

    private SiddhiContext siddhiContext = null;
    private String name;
    private boolean playback;
    private boolean enforceOrder;
    private boolean statsEnabled = false;
    private StatisticsManager statisticsManager = null;

    private ExecutorService executorService;
    private ScheduledExecutorService scheduledExecutorService;
    private List<EternalReferencedHolder> eternalReferencedHolders;
    private SnapshotService snapshotService;

    private ThreadBarrier threadBarrier = null;
    private TimestampGenerator timestampGenerator = null;
    private ElementIdGenerator elementIdGenerator;
    private Map<String, Script> scriptFunctionMap;
    private Map<String, StreamJunction> faultStreamMap =  new ConcurrentHashMap<String, StreamJunction>();;
    private ExceptionHandler<Object> disruptorExceptionHandler;
    private int bufferSize;
    private String siddhiAppString;
    private List<String> includedMetrics;
    private boolean transportChannelCreationEnabled;
    private List<Scheduler> schedulerList;

    public SiddhiAppContext() {
        this.eternalReferencedHolders = Collections.synchronizedList(new LinkedList<>());
        this.scriptFunctionMap = new HashMap<String, Script>();
        this.schedulerList = new ArrayList<Scheduler>();
    }

    public SiddhiContext getSiddhiContext() {
        return siddhiContext;
    }

    public void setSiddhiContext(SiddhiContext siddhiContext) {
        this.siddhiContext = siddhiContext;
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

    public boolean isStatsEnabled() {
        return statsEnabled;
    }

    public void setStatsEnabled(boolean statsEnabled) {
        this.statsEnabled = statsEnabled;
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

    public synchronized void addEternalReferencedHolder(EternalReferencedHolder eternalReferencedHolder) {
        eternalReferencedHolders.add(eternalReferencedHolder);
    }

    public List<EternalReferencedHolder> getEternalReferencedHolders() {
        return Collections.unmodifiableList(new ArrayList<>(eternalReferencedHolders));
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

    public ElementIdGenerator getElementIdGenerator() {
        return elementIdGenerator;
    }

    public void setElementIdGenerator(ElementIdGenerator elementIdGenerator) {
        this.elementIdGenerator = elementIdGenerator;
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

    public void setIncludedMetrics(List<String> includedMetrics) {
        this.includedMetrics = includedMetrics;
    }

    public List<String> getIncludedMetrics() {
        return includedMetrics;
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

    public void addFaultStreamJunction(String faultStreamName, StreamJunction streamJunction) {
        faultStreamMap.put(faultStreamName, streamJunction);
    }

    public StreamJunction getFaultStreamJunction(String faultStreamName) {
        return faultStreamMap.get(faultStreamName);
    }

    public List<Scheduler> getSchedulerList() {
        return schedulerList;
    }
}
