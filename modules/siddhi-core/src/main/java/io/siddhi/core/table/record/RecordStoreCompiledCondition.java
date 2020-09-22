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
package io.siddhi.core.table.record;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.util.collection.operator.CompiledCondition;

import java.util.Map;

/**
 * Compiled condition of the {@link AbstractRecordTable}
 */
public class RecordStoreCompiledCondition implements CompiledCondition {
    private static final long serialVersionUID = -8058614410325410636L;
    protected Map<String, ExpressionExecutor> variableExpressionExecutorMap;
    private CompiledCondition compiledCondition;
    private SiddhiQueryContext siddhiQueryContext;

    RecordStoreCompiledCondition(Map<String, ExpressionExecutor> variableExpressionExecutorMap,
                                 CompiledCondition compiledCondition,
                                 SiddhiQueryContext siddhiQueryContext) {
        this.variableExpressionExecutorMap = variableExpressionExecutorMap;
        this.compiledCondition = compiledCondition;
        this.siddhiQueryContext = siddhiQueryContext;
    }

    public CompiledCondition getCompiledCondition() {
        return compiledCondition;
    }

    public SiddhiQueryContext getSiddhiQueryContext() {
        return siddhiQueryContext;
    }

    public void setSiddhiAppContext(SiddhiAppContext siddhiAppContext) {
        siddhiQueryContext.setSiddhiAppContext(siddhiAppContext);
    }
}
