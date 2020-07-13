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

import io.siddhi.core.util.error.handler.util.ErroneousEventType;
import io.siddhi.core.util.error.handler.util.ErrorOccurrence;
import io.siddhi.core.util.error.handler.util.ErrorType;

/**
 * Represents an entry which represents an error, in the {@link io.siddhi.core.util.error.handler.store.ErrorStore}.
 */
public class ErrorEntry {
    private int id;
    private long timestamp;
    private String siddhiAppName;
    private String streamName;
    private byte[] eventAsBytes;
    private String cause;
    private String stackTrace;
    private String originalPayload;
    private ErrorOccurrence errorOccurrence;
    private ErroneousEventType eventType;
    private ErrorType errorType;

    public ErrorEntry(int id, long timestamp, String siddhiAppName, String streamName, byte[] eventAsBytes,
                      String cause, String stackTrace, String originalPayload, ErrorOccurrence errorOccurrence,
                      ErroneousEventType eventType, ErrorType errorType) {
        this.id = id;
        this.timestamp = timestamp;
        this.siddhiAppName = siddhiAppName;
        this.streamName = streamName;
        this.eventAsBytes = eventAsBytes;
        this.cause = cause;
        this.stackTrace = stackTrace;
        this.originalPayload = originalPayload;
        this.errorOccurrence = errorOccurrence;
        this.eventType = eventType;
        this.errorType = errorType;
    }

    public int getId() {
        return id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getSiddhiAppName() {
        return siddhiAppName;
    }

    public String getStreamName() {
        return streamName;
    }

    public byte[] getEventAsBytes() {
        return eventAsBytes;
    }

    public String getCause() {
        return cause;
    }

    public String getStackTrace() {
        return stackTrace;
    }

    public String getOriginalPayload() {
        return originalPayload;
    }

    public ErrorOccurrence getErrorOccurrence() {
        return errorOccurrence;
    }

    public ErroneousEventType getEventType() {
        return eventType;
    }

    public ErrorType getErrorType() {
        return errorType;
    }
}
