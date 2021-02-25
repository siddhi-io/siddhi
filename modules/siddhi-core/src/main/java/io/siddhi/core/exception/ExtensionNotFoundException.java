/*
 * Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.core.exception;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.query.api.SiddhiElement;

/**
 * This exception can be thrown when an extension is not found. E.g. The class loader cannot find it.
 */
public class ExtensionNotFoundException extends SiddhiAppCreationException {
    public ExtensionNotFoundException(String message, boolean isClassLoadingIssue) {
        super(message, isClassLoadingIssue);
    }

    public ExtensionNotFoundException(String message, Throwable throwable, boolean isClassLoadingIssue) {
        super(message, throwable, isClassLoadingIssue);
    }

    public ExtensionNotFoundException(String message) {
        super(message);
    }

    public ExtensionNotFoundException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public ExtensionNotFoundException(Throwable throwable) {
        super(throwable);
    }

    public ExtensionNotFoundException(String message, int[] queryContextStartIndex,
                                      int[] queryContextEndIndex, String siddhiAppName, String siddhiAppString) {
        super(message, queryContextStartIndex, queryContextEndIndex, siddhiAppName, siddhiAppString);
    }

    public ExtensionNotFoundException(String message, SiddhiElement siddhiElement,
                                      SiddhiAppContext siddhiAppContext) {
        super(message, siddhiElement, siddhiAppContext);
    }

    public ExtensionNotFoundException(String message, Throwable throwable,
                                      int[] queryContextStartIndex, int[] queryContextEndIndex) {
        super(message, throwable, queryContextStartIndex, queryContextEndIndex);
    }

    public ExtensionNotFoundException(String message, int[] queryContextStartIndex, int[] queryContextEndIndex) {
        super(message, queryContextStartIndex, queryContextEndIndex);
    }

    public ExtensionNotFoundException(String message, Throwable throwable,
                                      int[] queryContextStartIndex, int[] queryContextEndIndex,
                                      SiddhiAppContext siddhiAppContext) {
        super(message, throwable, queryContextStartIndex, queryContextEndIndex, siddhiAppContext);
    }

    public ExtensionNotFoundException(String message, Throwable throwable,
                                      int[] queryContextStartIndex, int[] queryContextEndIndex,
                                      String siddhiAppName, String siddhiAppString) {
        super(message, throwable, queryContextStartIndex, queryContextEndIndex, siddhiAppName, siddhiAppString);
    }
}
