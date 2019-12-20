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
import io.siddhi.core.query.processor.Processor;

/**
 * Created on 12/17/14.
 */
public interface PreStateProcessor extends Processor {

    void addState(StateEvent stateEvent);

    void addEveryState(StateEvent stateEvent);

    ComplexEventChunk processAndReturn(ComplexEventChunk complexEventChunk);

    int getStateId();

    void setStateId(int stateId);

    void init();

    boolean isStartState();

    void setStartState(boolean isStartState);

    void setWithinEveryPreStateProcessor(PreStateProcessor withinEveryPreStateProcessor);

    void updateState();

    void expireEvents(long timestamp);

    StreamPostStateProcessor getThisStatePostProcessor();

    void resetState();

    void setWithinTime(long withinTime);

    void setStartStateIds(int[] stateIds);

}
