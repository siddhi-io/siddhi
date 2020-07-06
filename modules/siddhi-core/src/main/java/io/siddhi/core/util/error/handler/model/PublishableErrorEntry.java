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

package io.siddhi.core.util.error.handler.model;

import com.lmax.disruptor.EventFactory;

/**
 * Denotes an erroneous event which can be published to the disruptor's ring buffer.
 */
public final class PublishableErrorEntry {

    public static final EventFactory<PublishableErrorEntry> EVENT_FACTORY = () -> new PublishableErrorEntry();

    private long timestamp;
    private String siddhiAppName;
    private String streamName;
    private byte[] eventAsBytes;
    private String cause;
    private byte[] stackTraceAsBytes;
    private byte[] originalPayloadAsBytes;
    private String errorOccurrence;
    private String eventType;
    private String errorType;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getSiddhiAppName() {
        return siddhiAppName;
    }

    public void setSiddhiAppName(String siddhiAppName) {
        this.siddhiAppName = siddhiAppName;
    }

    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    public byte[] getEventAsBytes() {
        return eventAsBytes;
    }

    public void setEventAsBytes(byte[] eventAsBytes) {
        this.eventAsBytes = eventAsBytes;
    }

    public String getCause() {
        return cause;
    }

    public void setCause(String cause) {
        this.cause = cause;
    }

    public byte[] getStackTraceAsBytes() {
        return stackTraceAsBytes;
    }

    public void setStackTraceAsBytes(byte[] stackTraceAsBytes) {
        this.stackTraceAsBytes = stackTraceAsBytes;
    }

    public byte[] getOriginalPayloadAsBytes() {
        return originalPayloadAsBytes;
    }

    public void setOriginalPayloadAsBytes(byte[] originalPayloadAsBytes) {
        this.originalPayloadAsBytes = originalPayloadAsBytes;
    }

    public String getErrorOccurrence() {
        return errorOccurrence;
    }

    public void setErrorOccurrence(String errorOccurrence) {
        this.errorOccurrence = errorOccurrence;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getErrorType() {
        return errorType;
    }

    public void setErrorType(String errorType) {
        this.errorType = errorType;
    }
}
