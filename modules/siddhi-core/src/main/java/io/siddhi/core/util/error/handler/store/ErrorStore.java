/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.core.util.error.handler.store;

import io.siddhi.core.util.error.handler.model.AbstractErrorEntry;
import io.siddhi.core.util.error.handler.model.ErroneousEvent;
import io.siddhi.core.util.error.handler.util.ErroneousEventType;

import java.util.List;
import java.util.Map;

/**
 * Denotes the interface for an error store in which, error event entries will be stored.
 */
public interface ErrorStore {
    void setProperties(Map properties);

    void saveBeforeSourceMappingError(String siddhiAppName, List<ErroneousEvent> erroneousEvents, String streamName);

    void saveOnSinkError(String siddhiAppName, ErroneousEvent erroneousEvent, ErroneousEventType eventType,
                         String streamName);

    void saveOnStreamError(String siddhiAppName, ErroneousEvent erroneousEvent, ErroneousEventType eventType,
                           String streamName);

    List<AbstractErrorEntry> loadErrorEntries(String siddhiAppName, Map<String, String> queryParams);

    void discardErroneousEvent(int id);
}
