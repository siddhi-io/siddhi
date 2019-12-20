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
package io.siddhi.core.event.stream;

import java.io.Serializable;


/**
 * The class which resembles an instance of operation performed on SnapshotableStreamEventQueue.
 */
public class Operation implements Serializable {

    public Operator operation;
    public Object parameters;

    public Operation(Operator operation, Object parameters) {
        this.operation = operation;
        this.parameters = parameters;
    }

    public Operation(Operator operation) {
        this.operation = operation;
    }

    /**
     * Possible Operator actions
     */
    public enum Operator {
        ADD,
        REMOVE,
        CLEAR,
        OVERWRITE,
        DELETE_BY_OPERATOR,
        DELETE_BY_INDEX
    }
}
