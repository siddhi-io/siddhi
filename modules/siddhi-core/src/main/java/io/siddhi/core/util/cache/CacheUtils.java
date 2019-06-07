/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.core.util.cache;

import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.util.collection.executor.AndMultiPrimaryKeyCollectionExecutor;
import io.siddhi.core.util.collection.executor.CompareCollectionExecutor;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.OverwriteTableIndexOperator;

import java.util.List;

/**
 * class containing utils related to store table cache
 */
public class CacheUtils {
    public static int findEventChunkSize(StreamEvent streamEvent) {
        int chunkSize = 1;
        if (streamEvent == null) {
            return 0;
        }
        StreamEvent streamEventCopy = streamEvent;
        while (streamEventCopy.hasNext()) {
            chunkSize = chunkSize + 1;
            streamEventCopy = streamEventCopy.getNext();
        }
        return chunkSize;
    }

    public static String getPrimaryKey(CompiledCondition compiledCondition, StateEvent matchingEvent) {
        StringBuilder primaryKey = new StringBuilder();
        StreamEvent streamEvent = matchingEvent.getStreamEvent(0);
        Object[] data = null;
        if (streamEvent != null) {
            data = streamEvent.getOutputData();
        }

        if (compiledCondition instanceof OverwriteTableIndexOperator) {
            OverwriteTableIndexOperator operator = (OverwriteTableIndexOperator) compiledCondition;
            if (operator.getCollectionExecutor() instanceof AndMultiPrimaryKeyCollectionExecutor) {
                AndMultiPrimaryKeyCollectionExecutor executor = (AndMultiPrimaryKeyCollectionExecutor) operator.
                        getCollectionExecutor();
                List<ExpressionExecutor> lis = executor.getMultiPrimaryKeyExpressionExecutors();
                for (ExpressionExecutor ee : lis) {
                    if (ee instanceof VariableExpressionExecutor) {
                        VariableExpressionExecutor vee = (VariableExpressionExecutor) ee;
                        primaryKey.append(data[vee.getPosition()[3]]);
                        primaryKey.append(":-:");
                    }
                }
            } else if (operator.getCollectionExecutor() instanceof CompareCollectionExecutor) {
                CompareCollectionExecutor executor = (CompareCollectionExecutor) operator.getCollectionExecutor();
                if (executor.getValueExpressionExecutor() instanceof ConstantExpressionExecutor) {
                    primaryKey.append(((ConstantExpressionExecutor) executor.getValueExpressionExecutor()).getValue());
                } else if (executor.getValueExpressionExecutor() instanceof VariableExpressionExecutor) {
                    VariableExpressionExecutor vee = (VariableExpressionExecutor) executor.getValueExpressionExecutor();
                    primaryKey.append(data[vee.getPosition()[3]]);
                } else {
                    return null;
                }
            }
        }
        return primaryKey.toString();
    }

    public static String getPrimaryKeyFromMatchingEvent(StateEvent matchingEvent) {
        StringBuilder primaryKey = new StringBuilder();
        StreamEvent streamEvent = matchingEvent.getStreamEvent(0);
        Object[] data = streamEvent.getOutputData();
        if (data.length == 1) {
            primaryKey.append(data[0]);
        } else {
            for (Object object : data) {
                primaryKey.append(object);
                primaryKey.append(":-:");
            }
        }
        return primaryKey.toString();
    }
}
