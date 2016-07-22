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
package org.wso2.siddhi.core.debugger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created on 7/20/16.
 */
public class QueryState {
    private HashMap<String, Object[]> unknownFields = new HashMap<String, Object[]>();
    private HashMap<String, Map<String, Object>> knownFields = new HashMap<String, Map<String, Object>>();

    public HashMap<String, Map<String, Object>> getKnownFields() {
        return knownFields;
    }

    public HashMap<String, Object[]> getUnknownFields() {
        return unknownFields;
    }

    public void setKnownFields(Map.Entry<String, Map<String, Object>> currentField) {
        this.knownFields.put(currentField.getKey(), currentField.getValue());
    }

    public void setUnknownFields(Map.Entry<String, Object[]> currentField) {
        this.unknownFields.put(currentField.getKey(),currentField.getValue());
    }

    public String toString() {
        StringBuilder stateRecord = new StringBuilder();
        if (unknownFields.size() != 0) {
            for (Map.Entry<String, Object[]> stringObjectEntry : unknownFields.entrySet()) {
                stateRecord.append(stringObjectEntry.getKey() + ": unknown field" + Arrays.deepToString(stringObjectEntry.getValue()) + "\n");
            }
        }
        if (knownFields.size() != 0) {
            for (Map.Entry<String, Map<String, Object>> entrySet : knownFields.entrySet()) {
                Map<String, Object> subMap = entrySet.getValue();
                String elementId = entrySet.getKey();
                stateRecord.append("For " + elementId + "\n");
                for (Map.Entry<String, Object> miniMapEntrySet : subMap.entrySet()) {
                    stateRecord.append(miniMapEntrySet.getKey() + ": " + miniMapEntrySet.getValue() + "\n");
                }
                stateRecord.append("\n");

            }
        }
        return stateRecord.toString();
    }
}
