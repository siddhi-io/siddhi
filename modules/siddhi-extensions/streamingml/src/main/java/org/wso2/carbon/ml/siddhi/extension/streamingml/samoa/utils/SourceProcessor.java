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

package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.EntranceProcessor;
import org.apache.samoa.core.Processor;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.Instances;
import org.apache.samoa.moa.options.AbstractOptionHandler;
import org.apache.samoa.streams.InstanceStream;
import org.apache.samoa.streams.StreamSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

/**
 * Source : https://github.com/apache/incubator-samoa/blob/master/samoa-api/src/main/java/org/
 * apache/samoa/streams/PrequentialSourceProcessor.java
 */

public abstract class SourceProcessor implements EntranceProcessor {

    private static final long serialVersionUID = 4169053337917578558L;
    private static final Logger logger = LoggerFactory.getLogger(SourceProcessor.class);
    protected transient ScheduledExecutorService timer;
    protected transient ScheduledFuture<?> schedule = null;
    protected int readyEventIndex = 1; // No waiting for the first event
    protected int delay;
    protected StreamSource streamSource;
    protected Instance firstInstance;
    protected int maxInstances;
    protected int batchSize;
    protected int numberOfInstancesSent = 0;
    protected boolean finished = false;
    protected boolean isInitialized = false;

    @Override
    public abstract ContentEvent nextEvent();

    @Override
    public abstract Processor newProcessor(Processor p);

    @Override
    public boolean process(ContentEvent event) {
        return false;
    }

    @Override
    public void onCreate(int id) {
        timer = Executors.newScheduledThreadPool(1);
        logger.info("Creating PrequentialSourceProcessor with processId {}", id);
    }

    @Override
    public boolean hasNext() {
        return !isFinished() && (delay <= 0 || numberOfInstancesSent < readyEventIndex);
    }

    @Override
    public boolean isFinished() {
        return hasReachedEndOfStream();
    }

    protected boolean hasReachedEndOfStream() {
        if (!streamSource.hasMoreInstances() || (maxInstances >= 0 &&
                numberOfInstancesSent >= maxInstances)) {
            finished = true;
            return true;
        }
        return false;
    }


    public void setStreamSource(InstanceStream stream) {
        if (stream instanceof AbstractOptionHandler) {
            ((AbstractOptionHandler) (stream)).prepareForUse();
        }
        this.streamSource = new StreamSource(stream);
        firstInstance = streamSource.nextInstance().getData();
    }

    protected Instance nextInstance() {
        if (this.isInitialized) {
            return streamSource.nextInstance().getData();
        } else {
            this.isInitialized = true;
            return firstInstance;
        }
    }

    protected void increaseReadyEventIndex() {
        readyEventIndex += batchSize;
        // if we exceed the max, cancel the timer
        if (schedule != null && isFinished()) {
            schedule.cancel(false);
        }
    }

    public Instances getDataset() {
        return firstInstance.dataset();
    }

    public void setMaxInstances(int value) {
        this.maxInstances = value;
    }

    public void setSourceDelay(int delay) {
        this.delay = delay;
    }

    public void setDelayBatchSize(int batch) {
        this.batchSize = batch;
    }

    public StreamSource getStreamSource() {
        return streamSource;
    }


}

