/*
 * Copyright (c)  2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.core;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.util.snapshot.SnapshotableElement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SnapshotableElementsHolder {
    private static final Logger LOGGER = Logger.getLogger(SnapshotableElementsHolder.class);
    private static final Map<String, SnapshotableElement> SNAPSHOTABLE_ELEMENT_MAP =
            new HashMap<String, SnapshotableElement>();
    private static Map<String, Map<String, Object>> existingStates = new HashMap<String, Map<String, Object>>();

    public static void putExistingState(String elementName, Map<String, Object> existingData) {
        existingStates.put(elementName, existingData);
    }

    public static void registerSnapshotableElement(SnapshotableElement element) {
        Map<String, Object> existingState = existingStates.get(element.getElementId());
        if (existingState != null) {
            element.restoreState(existingState);
            existingState.remove(element.getElementId());
        }
        SNAPSHOTABLE_ELEMENT_MAP.put(element.getElementId(), element);
        LOGGER.info("Snapshotable Element " + element.getElementId() + " registered successfully.");
    }

    public List<SnapshotableElement> getSnapshotableElements() {
        return new ArrayList<SnapshotableElement>(SNAPSHOTABLE_ELEMENT_MAP.values());
    }

    public SnapshotableElement getSnapshotableElement(String elementName) {
        return SNAPSHOTABLE_ELEMENT_MAP.get(elementName);
    }

    public static Map<String, Object> getState(String elementId) {
        Map<String, Object> state;
        if (SNAPSHOTABLE_ELEMENT_MAP.get(elementId) != null) {
            state = SNAPSHOTABLE_ELEMENT_MAP.get(elementId).currentState();
        } else {
            state = existingStates.get(elementId);
        }
        return state;
    }
}
