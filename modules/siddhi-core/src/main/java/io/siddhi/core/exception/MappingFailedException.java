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

package io.siddhi.core.exception;

import io.siddhi.core.util.error.handler.model.ErroneousEvent;

import java.util.List;

/**
 * Thrown during mapper level, when a payload is not able to be converted to a Siddhi event.
 */
public class MappingFailedException extends Exception {
    List<ErroneousEvent> failures;

    public MappingFailedException() {
        super();
    }

    public MappingFailedException(List<ErroneousEvent> failures) {
        this.failures = failures;
    }

    public MappingFailedException(String message) {
        super(message);
    }

    public MappingFailedException(List<ErroneousEvent> failures, String message) {
        super(message);
        this.failures = failures;
    }

    public MappingFailedException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public MappingFailedException(List<ErroneousEvent> failures, String message, Throwable cause) {
        super(message, cause);
        this.failures = failures;
    }

    public MappingFailedException(Throwable throwable) {
        super(throwable);
    }

    public MappingFailedException(List<ErroneousEvent> failures, Throwable cause) {
        super(cause);
        this.failures = failures;
    }

    public List<ErroneousEvent> getFailures() {
        return failures;
    }
}
