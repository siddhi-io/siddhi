/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.core.partition.executor;

import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.executor.ExpressionExecutor;

/**
 * Value partition executor computes the partition key based on value of given {@link ComplexEvent} attribute/s.
 */
public class ValuePartitionExecutor implements PartitionExecutor {

    private ExpressionExecutor expressionExecutor;

    public ValuePartitionExecutor(ExpressionExecutor expressionExecutor) {
        this.expressionExecutor = expressionExecutor;
    }

    public String execute(ComplexEvent event) {
        try {
            return expressionExecutor.execute(event).toString();
        } catch (NullPointerException ex) {
            return null;
        }
    }


}
