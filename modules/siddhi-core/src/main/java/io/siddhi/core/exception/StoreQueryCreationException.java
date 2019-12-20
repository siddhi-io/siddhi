/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import io.siddhi.query.api.exception.SiddhiAppContextException;

/**
 * Exception class to be used when an error occurs while calling on-demand queries.
 * This is deprecated and use OnDemandQueryCreationException instead.
 */
@Deprecated
public abstract class StoreQueryCreationException extends RuntimeException implements SiddhiAppContextException {

    protected StoreQueryCreationException(String message) {
        super(message);
    }

    protected StoreQueryCreationException(String message, Throwable throwable) {
        super(message, throwable);
    }

    protected StoreQueryCreationException(Throwable throwable) {
        super(throwable);
    }

    public abstract boolean isClassLoadingIssue();
}
