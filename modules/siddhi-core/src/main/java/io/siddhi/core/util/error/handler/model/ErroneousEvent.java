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

/**
 * Represents a collected event, which was dropped because it was erroneous.
 */
public class ErroneousEvent {
    private Object event;
    private Throwable throwable;
    private String cause;
    private Object originalPayload;

    public ErroneousEvent(Object event, Throwable throwable, String cause) {
        this.event = event;
        this.throwable = throwable;
        this.cause = cause;
    }

    public ErroneousEvent(Object event, String cause) {
        this.event = event;
        this.cause = cause;
    }

    public ErroneousEvent(Throwable throwable, String cause) {
        this.throwable = throwable;
        this.cause = cause;
    }

    public ErroneousEvent(String cause) {
        this.cause = cause;
    }

    public Object getEvent() {
        return event;
    }

    public String getCause() {
        return cause;
    }

    public Throwable getThrowable() {
        return throwable;
    }

    public Object getOriginalPayload() {
        return originalPayload;
    }

    public void setOriginalPayload(Object originalPayload) {
        this.originalPayload = originalPayload;
    }
}
