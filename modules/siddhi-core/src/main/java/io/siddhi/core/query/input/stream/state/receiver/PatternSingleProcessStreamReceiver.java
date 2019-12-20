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
import io.siddhi.core.query.input.SingleProcessStreamReceiver;

/**
 * {StreamJunction.Receiver} implementation to receive events into pattern queries
 * with single stream.
 */
public class PatternSingleProcessStreamReceiver extends SingleProcessStreamReceiver {

    public PatternSingleProcessStreamReceiver(String streamId, Object patternSyncObject,
                                              SiddhiQueryContext siddhiQueryContext) {
        super(streamId, patternSyncObject, siddhiQueryContext);
    }

    protected void stabilizeStates(long timestamp) {
        for (int i = 0; i < allStateProcessorsSize; i++) {
            allStateProcessors.get(i).expireEvents(timestamp);
        }
        if (stateProcessorsForStreamSize != 0) {
            stateProcessorsForStream.get(0).updateState();
        }
    }
}
