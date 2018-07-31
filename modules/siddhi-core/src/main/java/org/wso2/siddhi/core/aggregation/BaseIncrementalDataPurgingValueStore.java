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

package org.wso2.siddhi.core.aggregation;

import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventPool;

/**
 *
 * **/
public class BaseIncrementalDataPurgingValueStore {
    private StreamEventPool streamEventPool;
    private long timestamp;

    public BaseIncrementalDataPurgingValueStore(long timeStamp,
                                                StreamEventPool streamEventPool) {
        this.streamEventPool = streamEventPool;
        this.timestamp = timeStamp;

    }

    public StateEvent createStreamEvent(Object[] values) {
        StreamEvent streamEvent = streamEventPool.borrowEvent();
        streamEvent.setTimestamp(timestamp);
        streamEvent.setOutputData(values);

        StateEvent stateEvent = new StateEvent(2, 1);
        stateEvent.addEvent(0, streamEvent);
        return stateEvent;
    }
}
