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
package io.siddhi.core.util.parser;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.state.MetaStateEvent;
import io.siddhi.core.event.state.MetaStateEventAttribute;
import io.siddhi.core.event.state.populater.StateEventPopulatorFactory;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.selector.OptimisedJoinQuerySelector;
import io.siddhi.core.query.selector.QuerySelector;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.parser.helper.QueryParserHelper;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.expression.Variable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.siddhi.core.event.stream.MetaStreamEvent.EventType.TABLE;

/**
 * class to parse {@link OptimisedJoinQuerySelector}
 */
public class OptimisedJoinQuerySelectorParser {

    public static QuerySelector parse(QuerySelector querySelector, MetaStateEvent metaStateEvent,
                                      List<Attribute> expectedOutputAttributes, boolean isTableRightOfJoin,
                                      Map<String, Table> tableMap, boolean outputExpectsExpiredEvents,
                                      SiddhiQueryContext siddhiQueryContext) {

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
                    SiddhiConstants.UNKNOWN_STATE, tableMap, variableExpressionExecutors, false, 0,
                    ProcessingMode.BATCH, outputExpectsExpiredEvents, siddhiQueryContext);
            VariableExpressionExecutor executor = ((VariableExpressionExecutor) expressionExecutor);
            newMetaStateEvent.addOutputDataAllowingDuplicate(new MetaStateEventAttribute(executor
                        .getAttribute(), executor.getPosition()));
        }

        QueryParserHelper.updateVariablePosition(newMetaStateEvent, variableExpressionExecutors);

        OptimisedJoinQuerySelector optimisedJoinQuerySelector = new OptimisedJoinQuerySelector(querySelector.getId());
        optimisedJoinQuerySelector.setEventPopulator(
                StateEventPopulatorFactory.constructEventPopulator(newMetaStateEvent));

        return optimisedJoinQuerySelector;

    }
}
