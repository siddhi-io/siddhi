/*
 *  Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.core.exception;

/**
 * Represents an unchecked exception which may be thrown during runtime, from which we may not expect the Siddhi runtime
 * to reasonable recover.
 */
public class DatabaseRuntimeException extends SiddhiAppRuntimeException {

    private static final long serialVersionUID = 6844270338346626310L;

    public DatabaseRuntimeException(String message) {
        super(message);
    }

    public DatabaseRuntimeException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public DatabaseRuntimeException(Throwable throwable) {
        super(throwable);
    }

}
