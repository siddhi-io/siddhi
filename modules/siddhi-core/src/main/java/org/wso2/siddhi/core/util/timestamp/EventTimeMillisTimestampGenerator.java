/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.core.util.timestamp;

import java.util.ArrayList;
import java.util.List;

/**
 * Return the timestamp of the latest event received by the stream junction.
 *
 * @see org.wso2.siddhi.core.stream.StreamJunction#sendData(long, Object[])
 */
public class EventTimeMillisTimestampGenerator implements TimestampGenerator {

    /**
     * Timestamp of the last event received by StreamJunction.
     */
    private long latestTimestamp;

    /**
     * List of listeners listening to this timestamp generator.
     */
    private List<TimeChangeListener> timeChangeListeners = new ArrayList<TimeChangeListener>();

    @Override
    public long currentTime() {
        return latestTimestamp;
    }

    public void setTimestamp(long timestamp) {
        synchronized (this) {
            this.latestTimestamp = timestamp;
            for (TimeChangeListener listener : this.timeChangeListeners) {
                listener.onTimeChange(this.latestTimestamp);
            }
        }
    }

    public void addTimeChangeListener(TimeChangeListener listener) {
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
}
