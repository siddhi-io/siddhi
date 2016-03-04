package org.wso2.siddhi.extension.eventtable.sync.util;

public class ThrottleObject {
    private String throttleKey;
    private String throttleState;
    private String lastUpdatedTime;

    public String getThrottleKey() {
        return throttleKey;
    }

    public void setThrottleKey(String throttleKey) {
        this.throttleKey = throttleKey;
    }

    public String getThrottleState() {
        return throttleState;
    }

    public void setThrottleState(String throttleState) {
        this.throttleState = throttleState;
    }

    public String getLastUpdatedTime() {
        return lastUpdatedTime;
    }

    public void setLastUpdatedTime(String lastUpdatedTime) {
        this.lastUpdatedTime = lastUpdatedTime;
    }
}