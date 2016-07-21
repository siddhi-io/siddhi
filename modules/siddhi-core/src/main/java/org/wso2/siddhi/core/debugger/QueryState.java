package org.wso2.siddhi.core.debugger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by bhagya on 7/20/16.
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
