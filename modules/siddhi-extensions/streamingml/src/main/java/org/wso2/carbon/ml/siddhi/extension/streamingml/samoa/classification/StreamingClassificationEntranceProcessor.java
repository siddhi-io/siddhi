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

package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.EntranceProcessor;
import org.apache.samoa.core.Processor;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.Instances;
import org.apache.samoa.learners.InstanceContentEvent;
import org.apache.samoa.moa.options.AbstractOptionHandler;
import org.apache.samoa.streams.InstanceStream;
import org.apache.samoa.streams.StreamSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Source : https://github.com/apache/incubator-samoa/blob/master/samoa-api/src/main/java/org/apache/samoa/streams/PrequentialSourceProcessor.java
 */

public class StreamingClassificationEntranceProcessor implements EntranceProcessor {

    private static final long serialVersionUID = 4169053337917578558L;
    private static final Logger logger =
            LoggerFactory.getLogger(StreamingClassificationEntranceProcessor.class);
    private transient ScheduledExecutorService timer;
    private transient ScheduledFuture<?> schedule = null;
    private int readyEventIndex = 1; // No waiting for the first event
    private int delay;
    private StreamSource streamSource;
    private Instance firstInstance;
    private int maxInstances;
    private int batchSize;
    private int numberOfInstancesSent = 0;
    private boolean finished = false;
    private boolean isInited = false;


    @Override
    public ContentEvent nextEvent() {
        InstanceContentEvent contentEvent = null;
        if (hasReachedEndOfStream()) {
            contentEvent = new InstanceContentEvent(-1, firstInstance, false, true);
            contentEvent.setLast(true);
            // set finished status _after_ tagging last event
            logger.info("Finished !");
            finished = true;

        } else if (hasNext()) {
            numberOfInstancesSent++;
            Instance next = nextInstance();
            if (next.classValue() == -1.0) {     // If this event is a prediction event
                contentEvent = new InstanceContentEvent(numberOfInstancesSent, next, false, true); // This instance is only uses to test
            } else {                           //If it is not a prediction data then it use to train the model and test
                contentEvent = new InstanceContentEvent(numberOfInstancesSent, next, true, true);

            }
            // first call to this method will trigger the timer
            if (schedule == null && delay > 0) {
                schedule = timer.scheduleWithFixedDelay(new DelayTimeoutHandler(this), delay, delay,
                        TimeUnit.MICROSECONDS);
            }
        }
        return contentEvent;
    }

    private Instance nextInstance() {
        if (this.isInited) {
            return streamSource.nextInstance().getData();
        } else {
            this.isInited = true;
            return firstInstance;
        }
    }

    @Override
    public boolean process(ContentEvent event) {
        // TODO: possible refactor of the super-interface implementation
        // of source processor does not need this method
        return false;
    }

    @Override
    public void onCreate(int id) {
        timer = Executors.newScheduledThreadPool(1);
        logger.info("Creating PrequentialSourceProcessor with id {}", id);
    }

    @Override
    public Processor newProcessor(Processor p) {
        StreamingClassificationEntranceProcessor newProcessor =
                new StreamingClassificationEntranceProcessor();
        StreamingClassificationEntranceProcessor originProcessor =
                (StreamingClassificationEntranceProcessor) p;
        if (originProcessor.getStreamSource() != null) {
            newProcessor.setStreamSource(originProcessor.getStreamSource().getStream());
        }
        return newProcessor;
    }

    @Override
    public boolean hasNext() {
        return !isFinished() && (delay <= 0 || numberOfInstancesSent < readyEventIndex);
    }

    @Override
    public boolean isFinished() {
        return finished;
    }


    private boolean hasReachedEndOfStream() {
        return (!streamSource.hasMoreInstances() || (maxInstances >= 0 &&
                numberOfInstancesSent >= maxInstances));
    }

    public StreamSource getStreamSource() {
        return streamSource;
    }

    public void setStreamSource(InstanceStream stream) {
        if (stream instanceof AbstractOptionHandler) {
            ((AbstractOptionHandler) (stream)).prepareForUse();
        }
        this.streamSource = new StreamSource(stream);
        firstInstance = streamSource.nextInstance().getData();
    }

    public Instances getDataset() {
        return firstInstance.dataset();
    }


    private void increaseReadyEventIndex() {
        readyEventIndex += batchSize;
        // if we exceed the max, cancel the timer
        if (schedule != null && isFinished()) {
            schedule.cancel(false);
        }
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

    private class DelayTimeoutHandler implements Runnable {

        private StreamingClassificationEntranceProcessor processor;

        public DelayTimeoutHandler(StreamingClassificationEntranceProcessor processor) {
            this.processor = processor;
        }

        public void run() {
            processor.increaseReadyEventIndex();
        }
    }
}
