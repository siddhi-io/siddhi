/*
 * Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.core.aggregation.persistedaggregation;

import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.query.api.aggregation.TimePeriod;

/**
 * Holds the attributes to be stored in the queue
 **/
public class QueuedCudStreamProcessor {
    private Processor cudStreamProcessor;
    private StreamEvent streamEvent;
    private long startTimeOfNewAggregates;
    private long emittedTime;
    private String timeZone;
    private TimePeriod.Duration duration;

    public QueuedCudStreamProcessor(Processor cudStreamProcessor, StreamEvent streamEvent,
                                    long startTimeOfNewAggregates, long emittedTime, String timeZone,
                                    TimePeriod.Duration duration) {
        this.cudStreamProcessor = cudStreamProcessor;
        this.streamEvent = streamEvent;
        this.startTimeOfNewAggregates = startTimeOfNewAggregates;
        this.emittedTime = emittedTime;
        this.timeZone = timeZone;
        this.duration = duration;
    }

    public Processor getCudStreamProcessor() {

        return cudStreamProcessor;
    }

    public void setCudStreamProcessor(Processor cudStreamProcessor) {

        this.cudStreamProcessor = cudStreamProcessor;
    }

    public long getStartTimeOfNewAggregates() {

        return startTimeOfNewAggregates;
    }

    public void setStartTimeOfNewAggregates(long startTimeOfNewAggregates) {

        this.startTimeOfNewAggregates = startTimeOfNewAggregates;
    }

    public long getEmittedTime() {

        return emittedTime;
    }

    public void setEmittedTime(long emittedTime) {

        this.emittedTime = emittedTime;
    }

    public String getTimeZone() {

        return timeZone;
    }

    public void setTimeZone(String timeZone) {

        this.timeZone = timeZone;
    }

    public TimePeriod.Duration getDuration() {

        return duration;
    }

    public void setDuration(TimePeriod.Duration duration) {

        this.duration = duration;
    }

    public StreamEvent getStreamEvent() {

        return streamEvent;
    }

    public void setStreamEvent(StreamEvent streamEvent) {

        this.streamEvent = streamEvent;
    }
}
