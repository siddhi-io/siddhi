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

package io.siddhi.core.util.collection;

import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;

/**
 * Extract a {@link StreamEvent} from a {@link StateEvent}
 */
public class AddingStreamEventExtractor {
    private int matchingStreamEventPosition;

    public AddingStreamEventExtractor(int matchingStreamEventPosition) {
        this.matchingStreamEventPosition = matchingStreamEventPosition;
    }

    public StreamEvent getAddingStreamEvent(StateEvent updateOrAddingEvent) {
        return updateOrAddingEvent.getStreamEvent(matchingStreamEventPosition);
    }
}

