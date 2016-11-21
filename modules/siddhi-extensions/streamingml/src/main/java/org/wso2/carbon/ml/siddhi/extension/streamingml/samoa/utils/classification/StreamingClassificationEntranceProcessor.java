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

package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils.classification;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.learners.InstanceContentEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils.SourceProcessor;

import java.util.concurrent.TimeUnit;

public class StreamingClassificationEntranceProcessor extends SourceProcessor {

    private static final Logger logger =
            LoggerFactory.getLogger(StreamingClassificationEntranceProcessor.class);

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
                     // This instance is only uses for testing
                contentEvent = new InstanceContentEvent(numberOfInstancesSent, next, false, true);
            } else { //If it is not a prediction data then it use to train the model and test
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
