/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventPool;
import org.wso2.siddhi.core.event.stream.converter.ConversionStreamEventChunk;
import org.wso2.siddhi.core.event.stream.converter.StreamEventConverter;
import org.wso2.siddhi.core.query.input.stream.single.EntryValveProcessor;
import org.wso2.siddhi.core.util.lock.LockWrapper;
import org.wso2.siddhi.core.util.snapshot.Snapshotable;
import org.wso2.siddhi.core.util.statistics.LatencyTracker;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Scheduler implements Snapshotable {

    private static final Logger log = Logger.getLogger(Scheduler.class);
    private final BlockingQueue<Long> toNotifyQueue = new LinkedBlockingQueue<Long>();
    private final ThreadBarrier threadBarrier;
    private ScheduledExecutorService scheduledExecutorService;
    private EventCaller eventCaller;
    private volatile boolean running = false;
    private StreamEventPool streamEventPool;
    private ComplexEventChunk<StreamEvent> streamEventChunk;
    private ExecutionPlanContext executionPlanContext;
    private String elementId;
    private LatencyTracker latencyTracker;
    private LockWrapper lockWrapper;
    protected String queryName;


    public Scheduler(ScheduledExecutorService scheduledExecutorService, Schedulable singleThreadEntryValve, ExecutionPlanContext executionPlanContext) {
        this.threadBarrier = executionPlanContext.getThreadBarrier();
        this.scheduledExecutorService = scheduledExecutorService;
        this.eventCaller = new EventCaller(singleThreadEntryValve);
        this.executionPlanContext = executionPlanContext;
    }

    public void notifyAt(long time) {
        try {
            toNotifyQueue.put(time);

            if (!running && toNotifyQueue.size() == 1) {
                synchronized (toNotifyQueue) {
                    if (!running) {
                        running = true;
                        long timeDiff = time - System.currentTimeMillis(); //todo fix
                        if (timeDiff > 0) {
                            scheduledExecutorService.schedule(eventCaller, timeDiff, TimeUnit.MILLISECONDS);
                        } else {
                            scheduledExecutorService.schedule(eventCaller, 0, TimeUnit.MILLISECONDS);
                        }
                    }
                }
            }

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
            elementId = "Scheduler-"+executionPlanContext.getElementIdGenerator().createNewId();
        }
        executionPlanContext.getSnapshotService().addSnapshotable(queryName, this);
    }

    @Override
    public Object[] currentState() {
        return new Object[]{new AbstractMap.SimpleEntry<String, Object>("ToNotifyQueue", toNotifyQueue)};
    }

    @Override
    public void restoreState(Object[] state) {
        Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) state[0];
        BlockingQueue<Long> restoreToNotifyQueue = (BlockingQueue<Long>) stateEntry.getValue();
        for (Long time : restoreToNotifyQueue) {
            notifyAt(time);
        }
    }

    @Override
    public String getElementId() {
        return elementId;
    }

    public Scheduler clone(String key, EntryValveProcessor entryValveProcessor) {
        Scheduler scheduler = new Scheduler(scheduledExecutorService, entryValveProcessor, executionPlanContext);
        scheduler.elementId = elementId + "-" + key;
        return scheduler;
    }

    public void setLatencyTracker(LatencyTracker latencyTracker) {
        this.latencyTracker = latencyTracker;
    }

    private class EventCaller implements Runnable {
        private Schedulable singleThreadEntryValve;

        public EventCaller(Schedulable singleThreadEntryValve) {

            this.singleThreadEntryValve = singleThreadEntryValve;
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
        public void run() {
            try {
                Long toNotifyTime = toNotifyQueue.peek();
                long currentTime = System.currentTimeMillis();
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
                        if (latencyTracker != null) {
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
                    currentTime = System.currentTimeMillis();

                }
                if (toNotifyTime != null) {
                    scheduledExecutorService.schedule(eventCaller, toNotifyTime - currentTime, TimeUnit.MILLISECONDS);
                } else {
                    synchronized (toNotifyQueue) {
                        running = false;
                        if (toNotifyQueue.peek() != null) {
                            running = true;
                            scheduledExecutorService.schedule(eventCaller, 0, TimeUnit.MILLISECONDS);
                        }
                    }
                }

            } catch (Throwable t) {
                log.error(t);
            }
        }

    }
}
