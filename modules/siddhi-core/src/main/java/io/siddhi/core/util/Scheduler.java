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

package io.siddhi.core.util;

import com.google.common.collect.TreeMultimap;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.util.extension.holder.ExternalReferencedHolder;
import io.siddhi.core.util.lock.LockWrapper;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateHolder;
import io.siddhi.core.util.statistics.LatencyTracker;
import io.siddhi.core.util.statistics.metrics.Level;
import io.siddhi.core.util.timestamp.TimestampGeneratorImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Scheduler implementation to take periodic snapshots
 */
public class Scheduler implements ExternalReferencedHolder {

    private static final Logger log = LogManager.getLogger(Scheduler.class);
    private final ThreadBarrier threadBarrier;
    private final Schedulable singleThreadEntryValve;
    private final Semaphore mutex;
    protected String queryName;
    private SiddhiQueryContext siddhiQueryContext;
    private LockWrapper lockWrapper;
    private ScheduledExecutorService scheduledExecutorService;
    private StreamEventFactory streamEventFactory;
    private LatencyTracker latencyTracker;
    private StateHolder<SchedulerState> stateHolder;
    private boolean stop;


    public Scheduler(Schedulable singleThreadEntryValve, SiddhiQueryContext siddhiQueryContext) {
        this.threadBarrier = siddhiQueryContext.getSiddhiAppContext().getThreadBarrier();
        this.siddhiQueryContext = siddhiQueryContext;
        this.singleThreadEntryValve = singleThreadEntryValve;
        this.scheduledExecutorService = siddhiQueryContext.getSiddhiAppContext().getScheduledExecutorService();
        this.mutex = new Semaphore(1);

        siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator()
                .addTimeChangeListener(new TimestampGeneratorImpl.TimeChangeListener() {
                    @Override
                    public synchronized void onTimeChange(long currentTimestamp) {
                        Map<String, Map<String, SchedulerState>> allStates = stateHolder.getAllStates();
                        try {
                            TreeMultimap<Long, SchedulerState> sortedExpires = TreeMultimap.create();
                            for (Map.Entry<String, Map<String, SchedulerState>> allStatesEntry :
                                    allStates.entrySet()) {
                                for (Map.Entry<String, SchedulerState> stateEntry :
                                        allStatesEntry.getValue().entrySet()) {
                                    Long lastTime = stateEntry.getValue().toNotifyQueue.peek();
                                    if (lastTime != null && lastTime <= currentTimestamp) {
                                        sortedExpires.put(lastTime, stateEntry.getValue());
                                    }
                                }
                            }
                            for (Map.Entry<Long, SchedulerState> entry : sortedExpires.entries()) {
                                try {
                                    SiddhiAppContext.startPartitionFlow(entry.getValue().key);
                                    // If executed in a separate thread, while it is processing,
                                    // the new event will come into the window. As the result of it,
                                    // the window will emit the new event as an existing current event.
                                    sendTimerEvents(entry.getValue());
                                } finally {
                                    SiddhiAppContext.stopPartitionFlow();
                                }
                            }
                        } finally {
                            stateHolder.returnAllStates(allStates);
                        }
                    }
                });
    }

    public void init(LockWrapper lockWrapper, String queryName) {
        this.lockWrapper = lockWrapper;
        this.queryName = queryName;
        String id = "Scheduler_" + queryName + "_" + siddhiQueryContext.generateNewId();
        this.stateHolder = siddhiQueryContext.generateStateHolder(id, false, () -> new SchedulerState());
    }

    public void notifyAt(long time) {
        SchedulerState state = stateHolder.getState();
        try {
            // Insert the time into the queue
            state.toNotifyQueue.put(time);
            schedule(time, state, false);     // Let the subclasses to schedule the scheduler
        } catch (InterruptedException e) {
            // InterruptedException ignored if scheduledExecutorService has already been shutdown
            if (!scheduledExecutorService.isShutdown()) {
                log.error("Error when adding time:" + time + " to toNotifyQueue at Scheduler", e);
            }
        } finally {
            stateHolder.returnState(state);
        }
    }

    private void schedule(long time, SchedulerState state, boolean force) {
        if (!siddhiQueryContext.getSiddhiAppContext().isPlayback()) {
            if (!state.running && (state.toNotifyQueue.size() == 1 || force)) {
                try {
                    mutex.acquire();
                    if (!state.running) {
                        state.running = true;
                        long timeDiff = time - siddhiQueryContext.getSiddhiAppContext().
                                getTimestampGenerator().currentTime();
                        if (timeDiff > 0) {
                            state.scheduledFuture = scheduledExecutorService.schedule(state.eventCaller,
                                    timeDiff, TimeUnit.MILLISECONDS);
                        } else {
                            state.scheduledFuture = scheduledExecutorService.schedule(state.eventCaller,
                                    0, TimeUnit.MILLISECONDS);
                        }
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Error when scheduling System Time Based Scheduler", e);
                } finally {
                    mutex.release();
                }
            }
        }
    }


    public void setStreamEventFactory(StreamEventFactory streamEventFactory) {
        this.streamEventFactory = streamEventFactory;
    }

    public void setLatencyTracker(LatencyTracker latencyTracker) {
        this.latencyTracker = latencyTracker;
    }

    /**
     * Go through the timestamps stored in the toNotifyQueue and send the TIMER events for the expired events.
     *
     * @param state current state
     */
    private void sendTimerEvents(SchedulerState state) {
        Long toNotifyTime = state.toNotifyQueue.peek();
        long currentTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime();
        while (toNotifyTime != null && toNotifyTime - currentTime <= 0) {
            state.toNotifyQueue.poll();
            StreamEvent timerEvent = streamEventFactory.newInstance();
            timerEvent.setType(StreamEvent.Type.TIMER);
            timerEvent.setTimestamp(toNotifyTime);
            if (lockWrapper != null) {
                lockWrapper.lock();
            }
            threadBarrier.enter();
            try {
                ComplexEventChunk<StreamEvent> streamEventChunk = new ComplexEventChunk<>();
                streamEventChunk.add(timerEvent);
                if (Level.BASIC.compareTo(siddhiQueryContext.getSiddhiAppContext().getRootMetricsLevel()) <= 0 &&
                        latencyTracker != null) {
                    try {
                        latencyTracker.markIn();
                        singleThreadEntryValve.process(streamEventChunk);
                    } finally {
                        latencyTracker.markOut();
                    }
                } else {
                    singleThreadEntryValve.process(streamEventChunk);
                }
            } catch (Throwable t) {
                log.error("Error while sending timer events, " + t.getMessage(), t);
            } finally {
                if (lockWrapper != null) {
                    lockWrapper.unlock();
                }
                threadBarrier.exit();
            }

            toNotifyTime = state.toNotifyQueue.peek();
            currentTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime();
        }
    }

    /**
     * Schedule events which are not scheduled in the queue when switching back from event time to system current time
     */
    public void switchToLiveMode() {
        Map<String, Map<String, SchedulerState>> allStates = stateHolder.getAllStates();
        try {
            for (Map.Entry<String, Map<String, SchedulerState>> allStatesEntry : allStates.entrySet()) {
                for (Map.Entry<String, SchedulerState> stateEntry : allStatesEntry.getValue().entrySet()) {
                    Long toNotifyTime = stateEntry.getValue().toNotifyQueue.peek();
                    if (toNotifyTime != null) {
                        SiddhiAppContext.startPartitionFlow(allStatesEntry.getKey());
                        SiddhiAppContext.startGroupByFlow(stateEntry.getKey());
                        try {
                            schedule(toNotifyTime, stateEntry.getValue(), true);
                        } finally {
                            SiddhiAppContext.stopGroupByFlow();
                            SiddhiAppContext.stopPartitionFlow();
                        }
                    }
                }
            }
        } finally {
            stateHolder.returnAllStates(allStates);
        }
    }

    /**
     * this can be used to release
     * the acquired resources for processing.
     */
    public void switchToPlayBackMode() {
        Map<String, Map<String, SchedulerState>> allStates = stateHolder.getAllStates();
        try {
            for (Map.Entry<String, Map<String, SchedulerState>> allStatesEntry : allStates.entrySet()) {
                for (Map.Entry<String, SchedulerState> stateEntry : allStatesEntry.getValue().entrySet()) {
                    if (stateEntry.getValue().scheduledFuture != null) {
                        stateEntry.getValue().scheduledFuture.cancel(true);
                    }
                    //Make the scheduler running flag to false to make sure scheduler will schedule next time starts
                    stateEntry.getValue().running = false;
                }
            }
        } finally {
            stateHolder.returnAllStates(allStates);
        }
    }

    public void stop() {
        stop = true;
    }

    public void start() {
        stop = false;
    }

    private class EventCaller implements Runnable {
        private SchedulerState state;
        private String key;

        public EventCaller(SchedulerState state, String key) {
            this.state = state;
            this.key = key;
        }

        /**
         * When an object implementing interface <code>Runnable</code> is used
         * to create a thread, starting the thread causes the object's
         * <code>run</code> method to be called in that separately executing
         * thread.
         * <p>
         * The general contract of the method <code>run</code> is that it may
         * take any action whatsoever.
         *
         * @see Thread#run()
         */
        @Override
        public synchronized void run() {
            if (stop) {
                return;
            }
            SiddhiAppContext.startPartitionFlow(key);
            try {
                if (!siddhiQueryContext.getSiddhiAppContext().isPlayback()) {
                    sendTimerEvents(state);

                    Long toNotifyTime = state.toNotifyQueue.peek();
                    long currentTime = siddhiQueryContext.getSiddhiAppContext().getTimestampGenerator().currentTime();

                    if (toNotifyTime != null) {
                        state.scheduledFuture = scheduledExecutorService.
                                schedule(this, toNotifyTime - currentTime, TimeUnit.MILLISECONDS);
                    } else {
                        try {
                            mutex.acquire();
                            state.running = false;
                            if (state.toNotifyQueue.peek() != null) {
                                state.running = true;
                                state.scheduledFuture = scheduledExecutorService.schedule(this,
                                        0, TimeUnit.MILLISECONDS);
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            log.error("Error when scheduling System Time Based Scheduler", e);
                        } finally {
                            mutex.release();
                        }
                    }
                } else {
                    state.running = false;
                }
            } catch (Throwable t) {
                log.error("Error while executing Scheduled Timer Event Caller, " + t.getMessage(), t);
            } finally {
                SiddhiAppContext.stopPartitionFlow();
            }
        }

    }

    class SchedulerState extends State implements Comparable {

        private final BlockingQueue<Long> toNotifyQueue = new LinkedBlockingQueue<Long>();
        private final String key;
        private volatile boolean running = false;
        private EventCaller eventCaller;
        private ScheduledFuture scheduledFuture;

        public SchedulerState() {
            this.key = SiddhiAppContext.getPartitionFlowId();
            this.eventCaller = new EventCaller(this, key);
        }

        @Override
        public boolean canDestroy() {
            return toNotifyQueue.isEmpty() && (scheduledFuture == null || scheduledFuture.isDone());
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("ToNotifyQueue", toNotifyQueue);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            BlockingQueue<Long> restoreToNotifyQueue = (BlockingQueue<Long>) state.get("ToNotifyQueue");
            for (Long time : restoreToNotifyQueue) {
                notifyAt(time);
            }
        }

        @Override
        public int compareTo(Object o) {
            return 0;
        }
    }
}
