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

package io.siddhi.core.query.input.stream.state;

import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.query.api.execution.query.input.stream.StateInputStream;

/**
 * Created on 1/6/15.
 */
public class CountPostStateProcessor extends StreamPostStateProcessor {
    private final int minCount;
    private final int maxCount;

    public CountPostStateProcessor(int minCount, int maxCount) {

        this.minCount = minCount;
        this.maxCount = maxCount;
    }

    protected void process(StateEvent stateEvent, ComplexEventChunk complexEventChunk) {
        StreamEvent streamEvent = stateEvent.getStreamEvent(stateId);
        int streamEvents = 1;
        while (streamEvent.getNext() != null) {
            streamEvents++;
            streamEvent = streamEvent.getNext();
        }
        ((CountPreStateProcessor) thisStatePreProcessor).successCondition();
        stateEvent.setTimestamp(streamEvent.getTimestamp());

        if (streamEvents >= minCount) {

            if (thisStatePreProcessor.stateType == StateInputStream.Type.SEQUENCE) {
                if (nextStatePreProcessor != null) {
                    nextStatePreProcessor.addState(stateEvent);
                }
                if (streamEvents != maxCount) {
                    thisStatePreProcessor.addState(stateEvent);
                }
            } else if (streamEvents == minCount) {
                processMinCountReached(stateEvent, complexEventChunk);
            }
            if (streamEvents == maxCount) {
                thisStatePreProcessor.stateChanged();
            }
        }
    }

    public void processMinCountReached(StateEvent stateEvent, ComplexEventChunk complexEventChunk) {
        if (nextProcessor != null) {
            thisStatePreProcessor.stateChanged();
            complexEventChunk.reset();
            this.isEventReturned = true;
        }
        if (nextStatePreProcessor != null) {
            nextStatePreProcessor.addState(stateEvent);
        }
        if (nextEveryStatePreProcessor != null) {
            nextEveryStatePreProcessor.addEveryState(stateEvent);
        }
    }

    public void setNextStatePreProcessor(PreStateProcessor preStateProcessor) {
        this.nextStatePreProcessor = preStateProcessor;
        if (thisStatePreProcessor.isStartState &&
                thisStatePreProcessor.stateType == StateInputStream.Type.SEQUENCE &&
                minCount == 0) {
            preStateProcessor.getThisStatePostProcessor().setCallbackPreStateProcessor(
                    (CountPreStateProcessor) thisStatePreProcessor);
        }
    }
}
