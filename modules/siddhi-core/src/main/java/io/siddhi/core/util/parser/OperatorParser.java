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

package io.siddhi.core.util.parser;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.holder.SnapshotableStreamEventQueue;
import io.siddhi.core.exception.OperationNotSupportedException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.table.CacheTable;
import io.siddhi.core.table.Table;
import io.siddhi.core.table.holder.IndexedEventHolder;
import io.siddhi.core.util.collection.executor.CollectionExecutor;
import io.siddhi.core.util.collection.expression.AndMultiPrimaryKeyCollectionExpression;
import io.siddhi.core.util.collection.expression.AttributeCollectionExpression;
import io.siddhi.core.util.collection.expression.CollectionExpression;
import io.siddhi.core.util.collection.expression.CompareCollectionExpression;
import io.siddhi.core.util.collection.operator.CollectionOperator;
import io.siddhi.core.util.collection.operator.EventChunkOperator;
import io.siddhi.core.util.collection.operator.IndexOperator;
import io.siddhi.core.util.collection.operator.IndexOperatorForCache;
import io.siddhi.core.util.collection.operator.MapOperator;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.collection.operator.Operator;
import io.siddhi.core.util.collection.operator.OverwriteTableIndexOperator;
import io.siddhi.core.util.collection.operator.OverwriteTableIndexOperatorForCache;
import io.siddhi.core.util.collection.operator.SnapshotableEventQueueOperator;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.Variable;
import io.siddhi.query.api.expression.condition.Compare;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static io.siddhi.core.util.collection.expression.CollectionExpression.CollectionScope.INDEXED_RESULT_SET;
import static io.siddhi.core.util.collection.expression.CollectionExpression.CollectionScope.PRIMARY_KEY_RESULT_SET;

/**
 * Class to parse {@link Operator}
 */
public class OperatorParser {

    public static Operator constructOperator(Object storeEvents, Expression expression,
                                             MatchingMetaInfoHolder matchingMetaInfoHolder,
                                             List<VariableExpressionExecutor> variableExpressionExecutors,
                                             Map<String, Table> tableMap, SiddhiQueryContext siddhiQueryContext) {
        if (storeEvents instanceof IndexedEventHolder) {
            CollectionExpression collectionExpression = CollectionExpressionParser.parseCollectionExpression(
                    expression, matchingMetaInfoHolder, (IndexedEventHolder) storeEvents);
            CollectionExecutor collectionExecutor = CollectionExpressionParser.buildCollectionExecutor(
                    collectionExpression, matchingMetaInfoHolder, variableExpressionExecutors, tableMap,
                    true, ProcessingMode.BATCH, false, siddhiQueryContext, false, null);
            if (collectionExpression instanceof CompareCollectionExpression &&
                    ((CompareCollectionExpression) collectionExpression).getOperator() == Compare.Operator.EQUAL &&
                    (collectionExpression.getCollectionScope() == INDEXED_RESULT_SET ||
                            collectionExpression.getCollectionScope() == PRIMARY_KEY_RESULT_SET) &&
                    ((IndexedEventHolder) storeEvents).getPrimaryKeyReferenceHolders() != null &&
                    ((IndexedEventHolder) storeEvents).getPrimaryKeyReferenceHolders().length == 1 &&
                    ((IndexedEventHolder) storeEvents).getPrimaryKeyReferenceHolders()[0].getPrimaryKeyAttribute().
                            equals(((AttributeCollectionExpression)
                                    ((CompareCollectionExpression) collectionExpression)
                                            .getAttributeCollectionExpression()).getAttribute())) {
                return new OverwriteTableIndexOperator(collectionExecutor, siddhiQueryContext.getName());
            } else if (collectionExpression instanceof AndMultiPrimaryKeyCollectionExpression &&
                    collectionExpression.getCollectionScope() == PRIMARY_KEY_RESULT_SET) {
                return new OverwriteTableIndexOperator(collectionExecutor, siddhiQueryContext.getName());
            } else {
                return new IndexOperator(collectionExecutor, siddhiQueryContext.getName());
            }
        } else if (storeEvents instanceof ComplexEventChunk) {
            ExpressionExecutor expressionExecutor = ExpressionParser.parseExpression(expression,
                    matchingMetaInfoHolder.getMetaStateEvent(), matchingMetaInfoHolder.getCurrentState(), tableMap,
                    variableExpressionExecutors, false, 0,
                    ProcessingMode.BATCH, false, siddhiQueryContext);
            return new EventChunkOperator(expressionExecutor, matchingMetaInfoHolder.getStoreEventIndex());
        } else if (storeEvents instanceof SnapshotableStreamEventQueue) {
            ExpressionExecutor expressionExecutor = ExpressionParser.parseExpression(expression,
                    matchingMetaInfoHolder.getMetaStateEvent(), matchingMetaInfoHolder.getCurrentState(), tableMap,
                    variableExpressionExecutors, false, 0,
                    ProcessingMode.BATCH, false, siddhiQueryContext);
            return new SnapshotableEventQueueOperator(expressionExecutor, matchingMetaInfoHolder.getStoreEventIndex());
        } else if (storeEvents instanceof Map) {
            ExpressionExecutor expressionExecutor = ExpressionParser.parseExpression(expression,
                    matchingMetaInfoHolder.getMetaStateEvent(), matchingMetaInfoHolder.getCurrentState(), tableMap,
                    variableExpressionExecutors, false, 0,
                    ProcessingMode.BATCH, false, siddhiQueryContext);
            return new MapOperator(expressionExecutor, matchingMetaInfoHolder.getStoreEventIndex());
        } else if (storeEvents instanceof Collection) {
            ExpressionExecutor expressionExecutor = ExpressionParser.parseExpression(expression,
                    matchingMetaInfoHolder.getMetaStateEvent(), matchingMetaInfoHolder.getCurrentState(), tableMap,
                    variableExpressionExecutors, false, 0,
                    ProcessingMode.BATCH, false, siddhiQueryContext);
            return new CollectionOperator(expressionExecutor, matchingMetaInfoHolder.getStoreEventIndex());
        } else {
            throw new OperationNotSupportedException(storeEvents.getClass() + " is not supported by OperatorParser!");
        }
    }

    public static Operator constructOperatorForCache(Object storeEvents, Expression expression,
                                                     MatchingMetaInfoHolder matchingMetaInfoHolder,
                                                     List<VariableExpressionExecutor> variableExpressionExecutors,
                                                     Map<String, Table> tableMap, SiddhiQueryContext siddhiQueryContext,
                                                     boolean updateCachePolicyAttribute, CacheTable cacheTable) {
        if (storeEvents instanceof IndexedEventHolder && updateCachePolicyAttribute) {
            CollectionExpression collectionExpression = CollectionExpressionParser.parseCollectionExpression(
                    expression, matchingMetaInfoHolder, (IndexedEventHolder) storeEvents);
            CollectionExecutor collectionExecutor = CollectionExpressionParser.buildCollectionExecutor(
                    collectionExpression, matchingMetaInfoHolder, variableExpressionExecutors, tableMap,
                    true, ProcessingMode.BATCH, false, siddhiQueryContext, true, cacheTable);
            if (collectionExpression instanceof CompareCollectionExpression &&
                    ((CompareCollectionExpression) collectionExpression).getOperator() == Compare.Operator.EQUAL &&
                    (collectionExpression.getCollectionScope() == INDEXED_RESULT_SET ||
                            collectionExpression.getCollectionScope() == PRIMARY_KEY_RESULT_SET) &&
                    ((IndexedEventHolder) storeEvents).getPrimaryKeyReferenceHolders() != null &&
                    ((IndexedEventHolder) storeEvents).getPrimaryKeyReferenceHolders().length == 1 &&
                    ((IndexedEventHolder) storeEvents).getPrimaryKeyReferenceHolders()[0].getPrimaryKeyAttribute().
                            equals(((AttributeCollectionExpression)
                                    ((CompareCollectionExpression) collectionExpression)
                                            .getAttributeCollectionExpression()).getAttribute())) {

                return new OverwriteTableIndexOperatorForCache(collectionExecutor, siddhiQueryContext.getName(),
                        cacheTable);
            } else if (collectionExpression instanceof AndMultiPrimaryKeyCollectionExpression &&
                    collectionExpression.getCollectionScope() == PRIMARY_KEY_RESULT_SET) {
                return new OverwriteTableIndexOperatorForCache(collectionExecutor, siddhiQueryContext.getName(),
                        cacheTable);
            } else {
                return new IndexOperatorForCache(collectionExecutor, siddhiQueryContext.getName(), cacheTable);
            }
        }
        return constructOperator(storeEvents, expression, matchingMetaInfoHolder, variableExpressionExecutors,
                tableMap, siddhiQueryContext);
    }

    private static boolean isTableIndexVariable(MatchingMetaInfoHolder matchingMetaInfoHolder, Expression expression,
                                                String indexAttribute) {
        if (expression instanceof Variable) {
            Variable variable = (Variable) expression;
            if (variable.getStreamId() != null) {
                MetaStreamEvent tableStreamEvent = matchingMetaInfoHolder.getMetaStateEvent().getMetaStreamEvent
                        (matchingMetaInfoHolder.getStoreEventIndex());
                if (tableStreamEvent != null) {
                    if ((tableStreamEvent.getInputReferenceId() != null && variable.getStreamId().equals
                            (tableStreamEvent.getInputReferenceId())) ||
                            (tableStreamEvent.getLastInputDefinition().getId().equals(variable.getStreamId()))) {
                        if (Arrays.asList(tableStreamEvent.getLastInputDefinition().getAttributeNameArray()).contains
                                (indexAttribute)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }
}
