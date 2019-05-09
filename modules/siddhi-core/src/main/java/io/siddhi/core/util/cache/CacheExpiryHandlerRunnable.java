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

import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.table.InMemoryTable;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.parser.MatcherParser;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.Variable;
import io.siddhi.query.api.expression.condition.Compare;
import io.siddhi.query.api.expression.constant.LongConstant;
import io.siddhi.query.api.expression.math.Subtract;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CacheExpiryHandlerRunnable {
    private InMemoryTable cacheTable;
    private StreamEventFactory streamEventFactory;
    private MatchingMetaInfoHolder matchingMetaInfoHolder;
    private Expression rightExpressionForSubtract;
    private Expression rightExpressionForCompare;
    private Compare.Operator greaterThanOperator;
    private List<VariableExpressionExecutor> variableExpressionExecutors;
    private Map<String, Table> tableMap;

    public CacheExpiryHandlerRunnable(long expiryTime, InMemoryTable cacheTable, Map<String, Table> tableMap) {
        this.cacheTable = cacheTable;
        this.tableMap = tableMap;

        MetaStreamEvent tableMetaStreamEvent = new MetaStreamEvent();
        tableMetaStreamEvent.setEventType(MetaStreamEvent.EventType.TABLE);
        TableDefinition matchingTableDefinition = TableDefinition.id("");
        for (Attribute attribute : cacheTable.getTableDefinition().getAttributeList()) {
            tableMetaStreamEvent.addOutputData(attribute);
            matchingTableDefinition.attribute(attribute.getName(), attribute.getType());
        }
        tableMetaStreamEvent.addInputDefinition(matchingTableDefinition);
        streamEventFactory = new StreamEventFactory(tableMetaStreamEvent);

        rightExpressionForSubtract = new Variable("timestamp");
        ((Variable) rightExpressionForSubtract).setStreamId(cacheTable.getTableDefinition().getId());
        rightExpressionForCompare = new LongConstant(expiryTime);
        greaterThanOperator = Compare.Operator.GREATER_THAN;

        matchingMetaInfoHolder =
                MatcherParser.constructMatchingMetaStateHolder(tableMetaStreamEvent, 0,
                        cacheTable.getTableDefinition(), 0);
        variableExpressionExecutors = new ArrayList<>();
    }

    private ComplexEventChunk<StateEvent> generateDeleteEventChunk() {
        ComplexEventChunk<StateEvent> deleteEventChunk = new ComplexEventChunk<>();
        StateEvent stateEvent = new StateEvent(2, 0);
        stateEvent.addEvent(0, streamEventFactory.newInstance());
        deleteEventChunk.add(stateEvent);
        return deleteEventChunk;
    }

    private CompiledCondition generateExpiryCompiledCondition(long currentTime) {
        Expression leftExpressionForSubtract = new LongConstant(currentTime);
        Expression leftExpressionForCompare = new Subtract(leftExpressionForSubtract, rightExpressionForSubtract);
        Expression deleteCondition = new Compare(leftExpressionForCompare, greaterThanOperator,
                rightExpressionForCompare);
        return cacheTable.compileCondition(deleteCondition, matchingMetaInfoHolder, variableExpressionExecutors,
                tableMap, null);
    }

    public Runnable checkAndExpireCache = () -> cacheTable.delete(generateDeleteEventChunk(),
            generateExpiryCompiledCondition(new Timestamp(System.currentTimeMillis()).getTime()));
}
