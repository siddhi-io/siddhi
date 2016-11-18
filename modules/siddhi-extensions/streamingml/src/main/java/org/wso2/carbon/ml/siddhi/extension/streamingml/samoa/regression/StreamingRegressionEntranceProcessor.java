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

package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.regression;

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

public class StreamingRegressionEntranceProcessor implements EntranceProcessor {

    private static final long serialVersionUID = 4169053337917578558L;
    private static final Logger logger =
            LoggerFactory.getLogger(StreamingRegressionEntranceProcessor.class);
    private transient ScheduledExecutorService timer;
    private transient ScheduledFuture<?> schedule = null;
    private int readyEventIndex = 1;      // No waiting for the first event
    private int delay;
    private int batchSize;
    private boolean finished = false;
    private StreamSource streamSource;
    private Instance firstInstance;
    private boolean isInited = false;
    private int maxNumInstances;
    private int numInstanceSent = 0;
    protected InstanceStream sourceStream;

    @Override
    public ContentEvent nextEvent() {

        InstanceContentEvent contentEvent = null;
        if (hasReachedEndOfStream()) {
            contentEvent = new InstanceContentEvent(-1, firstInstance, false, true);
            contentEvent.setLast(true);
            // set finished status _after_ tagging last event
            finished = true;
        } else if (hasNext()) {
            numInstanceSent++;
            Instance next = nextInstance();
            Object a = next.classValue();
            if (a.toString().equals("-0.0")) {  // Check the last value of the event; If it equals -0.0 then use it as predicting event
                contentEvent = new InstanceContentEvent(numInstanceSent, next, false, true);
            } else {
                contentEvent = new InstanceContentEvent(numInstanceSent, next, true, true);
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
        logger.debug("Creating PrequentialSourceProcessor with id {}", id);
    }

    @Override
    public Processor newProcessor(Processor p) {
        StreamingRegressionEntranceProcessor newProcessor =
                new StreamingRegressionEntranceProcessor();
        StreamingRegressionEntranceProcessor originProcessor =
                (StreamingRegressionEntranceProcessor) p;
        if (originProcessor.getStreamSource() != null) {
            newProcessor.setStreamSource(originProcessor.getStreamSource().getStream());
        }
        return newProcessor;
    }

    @Override
    public boolean hasNext() {
        return !isFinished() && (delay <= 0 || numInstanceSent < readyEventIndex);
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    private boolean hasReachedEndOfStream() {
        return (!streamSource.hasMoreInstances() || (maxNumInstances >= 0 &&
                numInstanceSent >= maxNumInstances));
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
        if (firstInstance == null) {
            initStreamSource(sourceStream);
        }
        return firstInstance.dataset();
    }

    private void increaseReadyEventIndex() {
        readyEventIndex += batchSize;
        // if we exceed the max, cancel the timer
        if (schedule != null && isFinished()) {
            schedule.cancel(false);
        }
    }

    private void initStreamSource(InstanceStream stream) {
        if (stream instanceof AbstractOptionHandler) {
            ((AbstractOptionHandler) (stream)).prepareForUse();
        }

        this.streamSource = new StreamSource(stream);
        firstInstance = streamSource.nextInstance().getData();
    }

    public void setMaxNumInstances(int value) {
        maxNumInstances = value;
    }

    public void setSourceDelay(int delay) {
        this.delay = delay;
    }

    public void setDelayBatchSize(int batch) {
        this.batchSize = batch;
    }

    private class DelayTimeoutHandler implements Runnable {

        private StreamingRegressionEntranceProcessor processor;

        public DelayTimeoutHandler(StreamingRegressionEntranceProcessor processor) {
            this.processor = processor;
        }

        public void run() {
            processor.increaseReadyEventIndex();
        }
    }
}
