/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.core.query.input.stream.state;

import org.wso2.siddhi.core.query.processor.SchedulingProcessor;
import org.wso2.siddhi.core.util.extension.holder.EternalReferencedHolder;

/**
 * PreStateProcessor of events not received by Siddhi.
 */
public interface AbsentPreStateProcessor extends SchedulingProcessor, EternalReferencedHolder {

    /**
     * Returns the waiting time of the processor.
     *
     * @return -1 if the pattern is (not A and B) otherwise the 'for' time in milliseconds.
     */
    long getWaitingTime();

    /**
     * Check whether there are any pending events to process or arrive.
     *
     * @return true if not waiting for any events to arrive or process
     */
    boolean isEmpty();

    /**
     * Mark that there are no StateProcessors to process <b>events that arrive</b> before this processor.
     *
     * @param value
     */
    void setNoPresentBefore(boolean value);

    boolean isNoPresentBefore();

    void updateLastArrivalTime(long timestamp);
}
