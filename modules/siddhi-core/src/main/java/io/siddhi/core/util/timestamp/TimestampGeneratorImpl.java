/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.core.util.timestamp;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.util.Scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Class for timestamp generators. Provide event and system time generator based values.
 */
public class TimestampGeneratorImpl implements TimestampGenerator {

    /**
     * ScheduledExecutorService used to produce heartbeat.
     */
    private final ScheduledExecutorService scheduledExecutorService;
    /**
     * Timestamp as defined by the last event.
     */
    private long lastEventTimestamp;
    /**
     * The actual timestamp of last event arrival.
     */
    private long lastSystemTimestamp;
    /**
     * The minimum time to wait for new events to arrive.
     * If a new event does not arrive within this time, the generator
     * timestamp will be increased by incrementInMilliseconds.
     */
    private long idleTime = -1;
    /**
     * By how many milliseconds, the event timestamp should be increased.
     */
    private long incrementInMilliseconds;
    /**
     * A flag used to start the heartbeat clock for the first time only.
     */
    private boolean heartbeatRunning;

    /**
     * A callback to receive the ScheduledExecutorService notification and to increase the time if required.
     */
    private TimestampGeneratorImpl.TimeInjector timeInjector = new TimestampGeneratorImpl.TimeInjector();

    /**
     * List of listeners listening to this timestamp generator.
     */
    private List<TimestampGeneratorImpl.TimeChangeListener> timeChangeListeners = new ArrayList<>();

    private SiddhiAppContext siddhiAppContext;

    public TimestampGeneratorImpl(SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
        this.scheduledExecutorService = siddhiAppContext.getScheduledExecutorService();
    }

    @Override
    public long currentTime() {
        if (siddhiAppContext.isPlayback()) {
            return lastEventTimestamp;
        } else {
            return System.currentTimeMillis();
        }
    }

    /**
     * This method must be called within a synchronized block to avoid multiple schedulers from running simultaneously.
     *
     * @param duration delay the time from now to delay execution
     */
    private void notifyAfter(long duration) {
        if (!heartbeatRunning && idleTime != -1) {
            // Start the heartbeat if this is the first time
            scheduledExecutorService.schedule(timeInjector, duration, TimeUnit.MILLISECONDS);
            heartbeatRunning = true;
        }
    }

    /**
     * Set the timestamp and notify the interested listeners.
     *
     * @param timestamp the timestamp to the {@link TimestampGenerator}
     */
    @Override
    public void setCurrentTimestamp(long timestamp) {
        if (timestamp >= this.lastEventTimestamp) {
            synchronized (this) {
                if (timestamp >= this.lastEventTimestamp) {
                    // Update the time only if the time is greater than or equal to previous time
                    this.lastEventTimestamp = timestamp;

                    // Send a notification to listeners - Scheduler
                    for (TimestampGeneratorImpl.TimeChangeListener listener : this.timeChangeListeners) {
                        listener.onTimeChange(this.lastEventTimestamp);
                    }
                }
                // Schedule the heartbeat from the current event timestamp
                this.lastSystemTimestamp = System.currentTimeMillis();
                notifyAfter(idleTime);
            }
        }
    }

    /**
     * The {@link ScheduledExecutorService} waits until idleTime from the timestamp of last event
     * and if there are no new events arrived within that period, it will inject a new timestamp.
     *
     * @param idleTime the ideal time for wait until from the timestamp of last event.
     */
    @Override
    public void setIdleTime(long idleTime) {
        this.idleTime = idleTime;
    }

    /**
     * Set by how many milliseconds, the event timestamp should be increased.
     *
     * @param incrementInMilliseconds the timestamp incremental value.
     */
    @Override
    public void setIncrementInMilliseconds(long incrementInMilliseconds) {
        this.incrementInMilliseconds = incrementInMilliseconds;
    }

    /**
     * Register to listen for time changes.
     *
     * @param listener any listeners interested on time change.
     * @see Scheduler
     */
    @Override
    public void addTimeChangeListener(TimestampGeneratorImpl.TimeChangeListener listener) {
        synchronized (this) {
            this.timeChangeListeners.add(listener);
        }
    }

    /**
     * Listener used to get notification when a new event comes in.
     */
    public interface TimeChangeListener {
        void onTimeChange(long currentTimestamp);
    }

    /**
     * This {@link Runnable} class is executed by the {@link ScheduledExecutorService}
     */
    private class TimeInjector implements Runnable {
        @Override
        public void run() {
            long currentTimestamp = System.currentTimeMillis();
            synchronized (TimestampGeneratorImpl.this) {
                heartbeatRunning = false;
                long diff = currentTimestamp - lastSystemTimestamp;
                if (diff >= idleTime) {
                    // Siddhi has not received events for more than idleTime
                    long newTimestamp = lastEventTimestamp + incrementInMilliseconds;
                    setCurrentTimestamp(newTimestamp);
                } else {
                    // Wait for idleTime from the timestamp if last event arrival
                    notifyAfter(idleTime - diff);
                }
            }
        }
    }
}
