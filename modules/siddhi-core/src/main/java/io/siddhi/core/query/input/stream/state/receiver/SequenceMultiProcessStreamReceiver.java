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

package io.siddhi.core.query.input.stream.state.receiver;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.query.input.StateMultiProcessStreamReceiver;
import io.siddhi.core.query.input.stream.state.StateStreamRuntime;

/**
 * {StreamJunction.Receiver} implementation to receive events into sequence queries
 * with multiple streams.
 */
public class SequenceMultiProcessStreamReceiver extends StateMultiProcessStreamReceiver {

    private StateStreamRuntime stateStreamRuntime;

    public SequenceMultiProcessStreamReceiver(String streamId, int processCount, StateStreamRuntime
            stateStreamRuntime, Object patternSyncObject, SiddhiQueryContext siddhiQueryContext) {
        super(streamId, processCount, patternSyncObject, siddhiQueryContext);
        this.stateStreamRuntime = stateStreamRuntime;
        eventSequence = new int[processCount];
        int count = 0;
        for (int i = eventSequence.length - 1; i >= 0; i--) {
            eventSequence[count] = i;
            count++;
        }
    }

    protected void stabilizeStates(long timestamp) {
        for (int i = 0; i < allStateProcessorsSize; i++) {
            allStateProcessors.get(i).expireEvents(timestamp);
        }
        stateStreamRuntime.resetAndUpdate();
    }

}
