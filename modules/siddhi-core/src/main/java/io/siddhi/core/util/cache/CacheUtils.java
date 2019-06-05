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

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.ComplexEventChunk;
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

    public static void addRequiredFieldsToDataForCache(Object addingEventChunkForCache, Object event,
                                                       SiddhiAppContext siddhiAppContext, String cachePolicy,
                                                       boolean cacheExpiryEnabled) {
        if (event instanceof StreamEvent) {
            StreamEvent eventForCache = checkPoliocyAndAddFields(event, siddhiAppContext, cachePolicy,
                    cacheExpiryEnabled);
            ((ComplexEventChunk<StreamEvent>) addingEventChunkForCache).add(eventForCache);
        } else if (event instanceof StateEvent) {
            StreamEvent eventForCache = checkPoliocyAndAddFields(((StateEvent) event).getStreamEvent(0),
                    siddhiAppContext, cachePolicy, cacheExpiryEnabled);
            StateEvent stateEvent = new StateEvent(1, eventForCache.getOutputData().length);
            stateEvent.addEvent(0, eventForCache);
            ((ComplexEventChunk<StateEvent>) addingEventChunkForCache).add(stateEvent);
        }
    }

    private static StreamEvent checkPoliocyAndAddFields(Object event,
                                                 SiddhiAppContext siddhiAppContext, String cachePolicy,
                                                 boolean cacheExpiryEnabled) {
        Object[] outputDataForCache = null;
        Object[] outputData = ((StreamEvent) event).getOutputData();
        if (cachePolicy.equals("FIFO")) {
            outputDataForCache = new Object[outputData.length + 1];
            outputDataForCache[outputDataForCache.length - 1] =
                    siddhiAppContext.getTimestampGenerator().currentTime();
        } else if (cacheExpiryEnabled) {
            if (cachePolicy.equals("LRU")) {
                outputDataForCache = new Object[outputData.length + 2];
                outputDataForCache[outputDataForCache.length - 2] =
                        outputDataForCache[outputDataForCache.length - 1] =
                                siddhiAppContext.getTimestampGenerator().currentTime();
            } else if (cachePolicy.equals("LFU")) {
                outputDataForCache = new Object[outputData.length + 2];
                outputDataForCache[outputDataForCache.length - 2] =
                        siddhiAppContext.getTimestampGenerator().currentTime();
                outputDataForCache[outputDataForCache.length - 1] = 1;
            }
        } else {
            if (cachePolicy.equals("LRU")) {
                outputDataForCache = new Object[outputData.length + 1];
                outputDataForCache[outputDataForCache.length - 1] =
                        siddhiAppContext.getTimestampGenerator().currentTime();
            } else if (cachePolicy.equals("LFU")) {
                outputDataForCache = new Object[outputData.length + 1];
                outputDataForCache[outputDataForCache.length - 1] = 1;
            }
        }

        assert outputDataForCache != null;
        System.arraycopy(outputData, 0 , outputDataForCache, 0, outputData.length);
        StreamEvent eventForCache = new StreamEvent(0, 0, outputDataForCache.length);
        eventForCache.setOutputData(outputDataForCache);

        return eventForCache;
    }

//    public static int getReverseIndexOfData(String cachePolicy, boolean cacheExpiryEnabled, String dataColumn) {
//        if ()
//    }


    public static String getPrimaryKey(CompiledCondition compiledCondition, StateEvent matchingEvent) {
        StringBuilder primaryKey = new StringBuilder();
        StreamEvent streamEvent = matchingEvent.getStreamEvent(0);
        Object[] data = streamEvent.getOutputData();

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
                if (executor.getValueExpressionExecutor() instanceof VariableExpressionExecutor) {
                    VariableExpressionExecutor vee = (VariableExpressionExecutor) executor.getValueExpressionExecutor();
                    primaryKey.append(data[vee.getPosition()[3]]);
//                    primaryKey.append(":-:");
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

    public static String getPrimaryKeyFromCompileCondition(CompiledCondition compiledCondition) {
        StringBuilder primaryKey = new StringBuilder();
        if (compiledCondition instanceof OverwriteTableIndexOperator) {
            OverwriteTableIndexOperator operator = (OverwriteTableIndexOperator) compiledCondition;

            if (operator.getCollectionExecutor() instanceof AndMultiPrimaryKeyCollectionExecutor) {
                AndMultiPrimaryKeyCollectionExecutor executor = (AndMultiPrimaryKeyCollectionExecutor) operator.
                        getCollectionExecutor();
                List<ExpressionExecutor> lis = executor.getMultiPrimaryKeyExpressionExecutors();
                for (ExpressionExecutor ee : lis) {
                    if (ee instanceof ConstantExpressionExecutor) {
                        primaryKey.append(((ConstantExpressionExecutor) ee).getValue());
                        primaryKey.append(":-:");
                    }
                }
            } else if (operator.getCollectionExecutor() instanceof CompareCollectionExecutor) {
                CompareCollectionExecutor executor = (CompareCollectionExecutor) operator.getCollectionExecutor();
                if (executor.getValueExpressionExecutor() instanceof ConstantExpressionExecutor) {
                    primaryKey.append(((ConstantExpressionExecutor) executor.getValueExpressionExecutor()).getValue());
                } else {
                    return null;
                }
            }
            return primaryKey.toString();
        } else {
            return null;
        }
    }
}
