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
package org.wso2.siddhi.core.util.parser;


import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.state.MetaStateEvent;
import org.wso2.siddhi.core.event.state.MetaStateEventAttribute;
import org.wso2.siddhi.core.event.state.populater.StateEventPopulatorFactory;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.selector.OptimisedJoinQuerySelector;
import org.wso2.siddhi.core.query.selector.QuerySelector;
import org.wso2.siddhi.core.table.Table;
import org.wso2.siddhi.core.util.SiddhiConstants;
import org.wso2.siddhi.core.util.parser.helper.QueryParserHelper;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.execution.query.selection.Selector;
import org.wso2.siddhi.query.api.expression.Variable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.wso2.siddhi.core.event.stream.MetaStreamEvent.EventType.TABLE;


/**
 * class to parse {@link OptimisedJoinQuerySelector}
 */
public class OptimisedJoinQuerySelectorParser {

    public static QuerySelector parse(QuerySelector querySelector, Selector selector, MetaStateEvent metaStateEvent,
                                      List<Attribute> expectedOutputAttributes, boolean isTableRightOfJoin,
                                      Map<String, Table> tableMap, SiddhiAppContext siddhiAppContext,
                                      String queryName) {

        int storeIndex;
        if (isTableRightOfJoin) {
            storeIndex = 1;
        } else {
            storeIndex = 0;
        }

        MetaStreamEvent metaStoreEvent = new MetaStreamEvent();
        expectedOutputAttributes.forEach(metaStoreEvent::addOutputData);
        String tableReference = metaStateEvent.getMetaStreamEvent(storeIndex).getInputReferenceId();
        metaStoreEvent.setInputReferenceId(tableReference);
        StreamDefinition streamDefinition =  new StreamDefinition();
        streamDefinition.setId(metaStateEvent.getMetaStreamEvent(storeIndex).getLastInputDefinition().getId());
        expectedOutputAttributes
                .forEach((attribute) -> streamDefinition.attribute(attribute.getName(), attribute.getType()));
        metaStoreEvent.addInputDefinition(streamDefinition);
        metaStoreEvent.setEventType(TABLE);

        MetaStateEvent newMetaStateEvent = new MetaStateEvent(2);
        if (isTableRightOfJoin) {
            newMetaStateEvent.addEvent(metaStateEvent.getMetaStreamEvent(0));
            newMetaStateEvent.addEvent(metaStoreEvent);
        } else {
            newMetaStateEvent.addEvent(metaStoreEvent);
            newMetaStateEvent.addEvent(metaStateEvent.getMetaStreamEvent(1));
        }

        List<VariableExpressionExecutor> variableExpressionExecutors = new ArrayList<>();
        for (Attribute outputAttribute : expectedOutputAttributes) {
            Variable variable = new Variable(outputAttribute.getName());
            if (tableReference != null) {
                variable.setStreamId(tableReference);
            } else {
                variable.setStreamId(metaStateEvent.getMetaStreamEvent(storeIndex).getLastInputDefinition().getId());
            }
            ExpressionExecutor expressionExecutor = ExpressionParser.parseExpression(variable, newMetaStateEvent,
                    SiddhiConstants.UNKNOWN_STATE, tableMap, variableExpressionExecutors,siddhiAppContext, false, 0,
                    queryName);
            VariableExpressionExecutor executor = ((VariableExpressionExecutor) expressionExecutor);
            newMetaStateEvent.addOutputDataAllowingDuplicate(new MetaStateEventAttribute(executor
                    .getAttribute(), executor.getPosition()));
        }

        QueryParserHelper.updateVariablePosition(newMetaStateEvent, variableExpressionExecutors);

        OptimisedJoinQuerySelector optimisedJoinQuerySelector = new OptimisedJoinQuerySelector(querySelector.getId(),
                selector, querySelector.isCurrentOn(), querySelector.isExpiredOn(), siddhiAppContext);
        optimisedJoinQuerySelector.setEventPopulatorForOptimisedLookup(
                StateEventPopulatorFactory.constructEventPopulator(newMetaStateEvent));
        optimisedJoinQuerySelector.setAttributeProcessorList(querySelector.getAttributeProcessorList(),
                querySelector.isContainsAggregator());
        optimisedJoinQuerySelector.setHavingConditionExecutor(querySelector.getHavingConditionExecutor(),
                querySelector.isContainsAggregator());
        if (querySelector.isGroupBy()) {
            optimisedJoinQuerySelector.setGroupByKeyGenerator(querySelector.getGroupByKeyGenerator());
        }

        if (querySelector.isOrderBy()) {
            optimisedJoinQuerySelector.setOrderByEventComparator(querySelector.getOrderByEventComparator());
        }

        if (querySelector.getLimit() > 0) {
            optimisedJoinQuerySelector.setLimit(querySelector.getLimit());
        }

        if (querySelector.getOffset() > 0) {
            optimisedJoinQuerySelector.setOffset(querySelector.getOffset());
        }

        return optimisedJoinQuerySelector;

    }
}
