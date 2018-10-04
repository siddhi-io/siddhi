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

package org.wso2.siddhi.core.util;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventPool;
import org.wso2.siddhi.core.event.stream.converter.ConversionStreamEventChunk;
import org.wso2.siddhi.core.event.stream.converter.StreamEventConverter;
import org.wso2.siddhi.core.query.input.stream.single.EntryValveProcessor;
import org.wso2.siddhi.core.util.lock.LockWrapper;
import org.wso2.siddhi.core.util.snapshot.Snapshotable;
import org.wso2.siddhi.core.util.statistics.LatencyTracker;
import org.wso2.siddhi.core.util.timestamp.TimestampGeneratorImpl;

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
public class Scheduler implements Snapshotable {

    private static final Logger log = Logger.getLogger(Scheduler.class);
    private final BlockingQueue<Long> toNotifyQueue = new LinkedBlockingQueue<Long>();
    private final ThreadBarrier threadBarrier;
    private final Schedulable singleThreadEntryValve;
    private SiddhiAppContext siddhiAppContext;
    private String elementId;
    protected String queryName;
    private LockWrapper lockWrapper;
    private ScheduledExecutorService scheduledExecutorService;
    private EventCaller eventCaller;
    private final Semaphore mutex;
    private StreamEventPool streamEventPool;
    private ComplexEventChunk<StreamEvent> streamEventChunk;
    private LatencyTracker latencyTracker;
    private volatile boolean running = false;
    private ScheduledFuture scheduledFuture;


    public Scheduler(Schedulable singleThreadEntryValve, SiddhiAppContext siddhiAppContext) {
        this.threadBarrier = siddhiAppContext.getThreadBarrier();
        this.siddhiAppContext = siddhiAppContext;
        this.singleThreadEntryValve = singleThreadEntryValve;
        this.scheduledExecutorService = siddhiAppContext.getScheduledExecutorService();
        this.eventCaller = new EventCaller();
        mutex = new Semaphore(1);

        siddhiAppContext.getTimestampGenerator()
                .addTimeChangeListener(new TimestampGeneratorImpl.TimeChangeListener() {
                    @Override
                    public void onTimeChange(long currentTimestamp) {
                        Long lastTime = toNotifyQueue.peek();
                        if (lastTime != null && lastTime <= currentTimestamp) {
                            // If executed in a separate thread, while it is processing,
                            // the new event will come into the window. As the result of it,
                            // the window will emit the new event as an existing current event.
                            sendTimerEvents();
                        }
                    }
                });
    }

    public void schedule(long time) {
        if (!siddhiAppContext.isPlayback()) {
            if (!running && toNotifyQueue.size() == 1) {
                try {
                    mutex.acquire();
                    if (!running) {
                        running = true;
                        long timeDiff = time - siddhiAppContext.getTimestampGenerator().currentTime();
                        if (timeDiff > 0) {
                            scheduledFuture = scheduledExecutorService.schedule(eventCaller,
                                    timeDiff, TimeUnit.MILLISECONDS);
                        } else {
                            scheduledFuture = scheduledExecutorService.schedule(eventCaller,
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

    public Scheduler clone(String key, EntryValveProcessor entryValveProcessor) {
        Scheduler scheduler = new Scheduler(entryValveProcessor,
                siddhiAppContext);
        scheduler.elementId = elementId + "-" + key;
        return scheduler;
    }

    public void notifyAt(long time) {
        try {
            // Insert the time into the queue
            toNotifyQueue.put(time);
            schedule(time);     // Let the subclasses to schedule the scheduler
        } catch (InterruptedException e) {
            log.error("Error when adding time:" + time + " to toNotifyQueue at Scheduler", e);
        }
    }

    public void setStreamEventPool(StreamEventPool streamEventPool) {
        this.streamEventPool = streamEventPool;
        streamEventChunk = new ConversionStreamEventChunk((StreamEventConverter) null, streamEventPool);
    }

    public void init(LockWrapper lockWrapper, String queryName) {
        this.lockWrapper = lockWrapper;
        this.queryName = queryName;
        if (elementId == null) {
            elementId = "Scheduler-" + siddhiAppContext.getElementIdGenerator().createNewId();
        }
        siddhiAppContext.getSnapshotService().addSnapshotable(queryName, this);
    }

    @Override
    public Map<String, Object> currentState() {
        Map<String, Object> state = new HashMap<>();
        state.put("ToNotifyQueue", toNotifyQueue);
        return state;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        BlockingQueue<Long> restoreToNotifyQueue = (BlockingQueue<Long>) state.get("ToNotifyQueue");
        for (Long time : restoreToNotifyQueue) {
            notifyAt(time);
        }
    }

    @Override
    public String getElementId() {
        return elementId;
    }

    public void setLatencyTracker(LatencyTracker latencyTracker) {
        this.latencyTracker = latencyTracker;
    }

    /**
     * Go through the timestamps stored in the {@link #toNotifyQueue} and send the TIMER events for the expired events.
     */
    protected void sendTimerEvents() {
        Long toNotifyTime = toNotifyQueue.peek();
        long currentTime = siddhiAppContext.getTimestampGenerator().currentTime();
        while (toNotifyTime != null && toNotifyTime - currentTime <= 0) {
            toNotifyQueue.poll();

            StreamEvent timerEvent = streamEventPool.borrowEvent();
            timerEvent.setType(StreamEvent.Type.TIMER);
            timerEvent.setTimestamp(toNotifyTime);
            streamEventChunk.add(timerEvent);
            if (lockWrapper != null) {
                lockWrapper.lock();
            }
            threadBarrier.pass();
            try {
                if (siddhiAppContext.isStatsEnabled() && latencyTracker != null) {
                    try {
                        latencyTracker.markIn();
                        singleThreadEntryValve.process(streamEventChunk);
                    } finally {
                        latencyTracker.markOut();
                    }
                } else {
                    singleThreadEntryValve.process(streamEventChunk);
                }
            } finally {
                if (lockWrapper != null) {
                    lockWrapper.unlock();
                }
            }
            streamEventChunk.clear();

            toNotifyTime = toNotifyQueue.peek();
            currentTime = siddhiAppContext.getTimestampGenerator().currentTime();
        }
    }

    /**
     * Schedule events which are not scheduled in the queue when switching back from event time to system current time
     */
    public void switchToLiveMode() {
        Long toNotifyTime = toNotifyQueue.peek();
        if (toNotifyTime != null) {
            schedule(toNotifyTime);
        }
    }

    /**
     * this can be used to release
     * the acquired resources for processing.
     */
    public void switchToPlayBackMode() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
        //Make the scheduler running flag to false to make sure scheduler will schedule next time starts
        running = false;
    }

    private class EventCaller implements Runnable {
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
        public void run() {
            try {
                if (!siddhiAppContext.isPlayback()) {
                    sendTimerEvents();

                    Long toNotifyTime = toNotifyQueue.peek();
                    long currentTime = siddhiAppContext.getTimestampGenerator().currentTime();

                    if (toNotifyTime != null) {
                        scheduledFuture = scheduledExecutorService.
                                schedule(eventCaller, toNotifyTime - currentTime, TimeUnit.MILLISECONDS);
                    } else {
                        try {
                            mutex.acquire();
                            running = false;
                            if (toNotifyQueue.peek() != null) {
                                running = true;
                                scheduledFuture = scheduledExecutorService.schedule(eventCaller,
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
                    running = false;
                }
            } catch (Throwable t) {
                log.error(t);
            }
        }

    }
}
