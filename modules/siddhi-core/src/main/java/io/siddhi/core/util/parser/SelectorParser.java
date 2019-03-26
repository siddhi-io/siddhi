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
import io.siddhi.core.event.MetaComplexEvent;
import io.siddhi.core.event.state.MetaStateEvent;
import io.siddhi.core.event.state.MetaStateEventAttribute;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.executor.condition.ConditionExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.selector.GroupByKeyGenerator;
import io.siddhi.core.query.selector.OrderByEventComparator;
import io.siddhi.core.query.selector.QuerySelector;
import io.siddhi.core.query.selector.attribute.processor.AttributeProcessor;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.exception.DuplicateAttributeException;
import io.siddhi.query.api.execution.query.output.stream.OutputStream;
import io.siddhi.query.api.execution.query.selection.OutputAttribute;
import io.siddhi.query.api.execution.query.selection.Selector;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.Variable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Class to parse {@link QuerySelector}.
 */
public class SelectorParser {
    private static final ThreadLocal<String> containsAggregatorThreadLocal = new ThreadLocal<String>();

    /**
     * Parse Selector portion of a query and return corresponding QuerySelector.
     *
     * @param selector                    selector to be parsed
     * @param outputStream                output stream
     * @param metaComplexEvent            Meta event used to collect execution info of stream associated with query
     * @param tableMap                    Table Map
     * @param variableExpressionExecutors variable expression executors
     * @param metaPosition                helps to identify the meta position of aggregates
     * @param processingMode              processing mode of the query
     * @param outputExpectsExpiredEvents  is expired events sent as output
     * @param siddhiQueryContext          current siddhi query context
     * @return QuerySelector
     */
    public static QuerySelector parse(Selector selector, OutputStream outputStream,
                                      MetaComplexEvent metaComplexEvent, Map<String, Table> tableMap,
                                      List<VariableExpressionExecutor> variableExpressionExecutors,
                                      int metaPosition, ProcessingMode processingMode,
                                      boolean outputExpectsExpiredEvents, SiddhiQueryContext siddhiQueryContext) {
        boolean currentOn = false;
        boolean expiredOn = false;
        String id = null;

        if (outputStream.getOutputEventType() == OutputStream.OutputEventType.CURRENT_EVENTS || outputStream
                .getOutputEventType() == OutputStream.OutputEventType.ALL_EVENTS) {
            currentOn = true;
        }
        if (outputStream.getOutputEventType() == OutputStream.OutputEventType.EXPIRED_EVENTS || outputStream
                .getOutputEventType() == OutputStream.OutputEventType.ALL_EVENTS) {
            expiredOn = true;
        }
        boolean groupBy = !selector.getGroupByList().isEmpty();
        id = outputStream.getId();
        containsAggregatorThreadLocal.remove();
        QuerySelector querySelector = new QuerySelector(id, selector, currentOn, expiredOn, siddhiQueryContext);
        List<AttributeProcessor> attributeProcessors = getAttributeProcessors(selector, id,
                metaComplexEvent, tableMap, variableExpressionExecutors, outputStream, metaPosition,
                processingMode, outputExpectsExpiredEvents, groupBy, siddhiQueryContext);
        querySelector.setAttributeProcessorList(attributeProcessors,
                "true".equals(containsAggregatorThreadLocal.get()));
        containsAggregatorThreadLocal.remove();
        ConditionExpressionExecutor havingCondition = generateHavingExecutor(selector.getHavingExpression(),
                metaComplexEvent, tableMap, variableExpressionExecutors, siddhiQueryContext);
        querySelector.setHavingConditionExecutor(havingCondition, "true".equals(containsAggregatorThreadLocal.get()));
        containsAggregatorThreadLocal.remove();
        if (!selector.getGroupByList().isEmpty()) {
            List<Expression> groupByExpressionList = selector.getGroupByList().stream()
                    .map(groupByVariable -> (Expression) groupByVariable)
                    .collect(Collectors.toList());
            querySelector.setGroupByKeyGenerator(new GroupByKeyGenerator(groupByExpressionList, metaComplexEvent,
                    SiddhiConstants.UNKNOWN_STATE, null, variableExpressionExecutors,
                    siddhiQueryContext));
        }
        if (!selector.getOrderByList().isEmpty()) {
            querySelector.setOrderByEventComparator(new OrderByEventComparator(selector.getOrderByList(),
                    metaComplexEvent, SiddhiConstants.HAVING_STATE, null, variableExpressionExecutors,
                    siddhiQueryContext));
        }
        if (selector.getLimit() != null) {
            ExpressionExecutor expressionExecutor = ExpressionParser.parseExpression((Expression) selector.getLimit(),
                    metaComplexEvent, SiddhiConstants.HAVING_STATE, tableMap, variableExpressionExecutors,
                    false, 0,
                    ProcessingMode.BATCH, false, siddhiQueryContext);
            containsAggregatorThreadLocal.remove();
            querySelector.setLimit(((Number)
                    (((ConstantExpressionExecutor) expressionExecutor).getValue())).longValue());
        }
        if (selector.getOffset() != null) {
            ExpressionExecutor expressionExecutor = ExpressionParser.parseExpression((Expression) selector.getOffset(),
                    metaComplexEvent, SiddhiConstants.HAVING_STATE, tableMap, variableExpressionExecutors,
                    false, 0,
                    ProcessingMode.BATCH, false, siddhiQueryContext);
            containsAggregatorThreadLocal.remove();
            querySelector.setOffset(((Number)
                    (((ConstantExpressionExecutor) expressionExecutor).getValue())).longValue());
        }
        return querySelector;
    }

    /**
     * Method to construct AttributeProcessor list for the selector.
     *
     * @param selector                    Selector
     * @param id                          stream id
     * @param metaComplexEvent            meta ComplexEvent
     * @param tableMap                    Table Map
     * @param variableExpressionExecutors list of VariableExpressionExecutors
     * @param outputStream                output stream
     * @param processingMode              processing mode of the query
     * @param outputExpectsExpiredEvents  is expired events sent as output
     * @param groupBy                     is Attributes groupBy
     * @param siddhiQueryContext          current siddhi query context  @return list of AttributeProcessors
     */
    private static List<AttributeProcessor> getAttributeProcessors(Selector selector, String id,
                                                                   MetaComplexEvent metaComplexEvent,
                                                                   Map<String, Table> tableMap,
                                                                   List<VariableExpressionExecutor>
                                                                           variableExpressionExecutors,
                                                                   OutputStream outputStream,
                                                                   int metaPosition,
                                                                   ProcessingMode processingMode,
                                                                   boolean outputExpectsExpiredEvents,
                                                                   boolean groupBy,
                                                                   SiddhiQueryContext siddhiQueryContext) {

        List<AttributeProcessor> attributeProcessorList = new ArrayList<>();
        StreamDefinition outputDefinition = StreamDefinition.id(id);
        outputDefinition.setQueryContextStartIndex(outputStream.getQueryContextStartIndex());
        outputDefinition.setQueryContextEndIndex(outputStream.getQueryContextEndIndex());
        List<OutputAttribute> outputAttributes = selector.getSelectionList();
        if (selector.getSelectionList().size() == 0) {
            if (metaComplexEvent instanceof MetaStreamEvent) {

                List<Attribute> attributeList = ((MetaStreamEvent) metaComplexEvent).getLastInputDefinition()
                        .getAttributeList();
                for (Attribute attribute : attributeList) {
                    Variable variable = new Variable(attribute.getName());
                    variable.setQueryContextStartIndex(selector.getQueryContextStartIndex());
                    variable.setQueryContextEndIndex(selector.getQueryContextEndIndex());
                    OutputAttribute outputAttribute = new OutputAttribute(variable);
                    outputAttribute.setQueryContextStartIndex(selector.getQueryContextStartIndex());
                    outputAttribute.setQueryContextEndIndex(selector.getQueryContextEndIndex());
                    outputAttributes.add(outputAttribute);
                }
            } else {
                int position = 0;
                for (MetaStreamEvent metaStreamEvent : ((MetaStateEvent) metaComplexEvent).getMetaStreamEvents()) {
                    if (metaPosition == SiddhiConstants.UNKNOWN_STATE || metaPosition == position) {
                        List<Attribute> attributeList = metaStreamEvent.getLastInputDefinition().getAttributeList();
                        for (Attribute attribute : attributeList) {
                            Variable variable = new Variable(attribute.getName());
                            variable.setQueryContextStartIndex(selector.getQueryContextStartIndex());
                            variable.setQueryContextEndIndex(selector.getQueryContextEndIndex());
                            OutputAttribute outputAttribute = new OutputAttribute(variable);
                            outputAttribute.setQueryContextStartIndex(selector.getQueryContextStartIndex());
                            outputAttribute.setQueryContextEndIndex(selector.getQueryContextEndIndex());
                            if (!outputAttributes.contains(outputAttribute)) {
                                outputAttributes.add(outputAttribute);
                            } else {
                                List<AbstractDefinition> definitions = new ArrayList<>();
                                for (MetaStreamEvent aMetaStreamEvent : ((MetaStateEvent) metaComplexEvent)
                                        .getMetaStreamEvents()) {
                                    definitions.add(aMetaStreamEvent.getLastInputDefinition());
                                }
                                throw new DuplicateAttributeException("Duplicate attribute exist in streams " +
                                        definitions, outputStream.getQueryContextStartIndex(),
                                        outputStream.getQueryContextEndIndex());
                            }
                        }
                    }
                    ++position;
                }
            }
        }

        int i = 0;
        for (OutputAttribute outputAttribute : outputAttributes) {

            ExpressionExecutor expressionExecutor = ExpressionParser.parseExpression(outputAttribute.getExpression(),
                    metaComplexEvent, SiddhiConstants.UNKNOWN_STATE, tableMap, variableExpressionExecutors,
                    groupBy, 0, processingMode,
                    outputExpectsExpiredEvents, siddhiQueryContext);
            if (expressionExecutor instanceof VariableExpressionExecutor) {   //for variables we will directly put
                // value at conversion stage
                VariableExpressionExecutor executor = ((VariableExpressionExecutor) expressionExecutor);
                if (metaComplexEvent instanceof MetaStateEvent) {
                    ((MetaStateEvent) metaComplexEvent).addOutputDataAllowingDuplicate(new MetaStateEventAttribute(executor
                            .getAttribute(), executor.getPosition()));
                } else {
                    ((MetaStreamEvent) metaComplexEvent).addOutputDataAllowingDuplicate(executor.getAttribute());
                }
                outputDefinition.attribute(outputAttribute.getRename(), ((VariableExpressionExecutor)
                        expressionExecutor).getAttribute().getType());
            } else {
                //To maintain output variable positions
                if (metaComplexEvent instanceof MetaStateEvent) {
                    ((MetaStateEvent) metaComplexEvent).addOutputDataAllowingDuplicate(null);
                } else {
                    ((MetaStreamEvent) metaComplexEvent).addOutputDataAllowingDuplicate(null);
                }
                AttributeProcessor attributeProcessor = new AttributeProcessor(expressionExecutor);
                attributeProcessor.setOutputPosition(i);
                attributeProcessorList.add(attributeProcessor);
                outputDefinition.attribute(outputAttribute.getRename(), attributeProcessor.getOutputType());
            }
            i++;
        }
        metaComplexEvent.setOutputDefinition(outputDefinition);
        return attributeProcessorList;
    }

    private static ConditionExpressionExecutor generateHavingExecutor(Expression expression,
                                                                      MetaComplexEvent metaComplexEvent,
                                                                      Map<String, Table> tableMap,
                                                                      List<VariableExpressionExecutor>
                                                                              variableExpressionExecutors, SiddhiQueryContext siddhiQueryContext) {
        ConditionExpressionExecutor havingConditionExecutor = null;
        if (expression != null) {
            havingConditionExecutor = (ConditionExpressionExecutor) ExpressionParser.parseExpression(expression,
                    metaComplexEvent, SiddhiConstants.HAVING_STATE, tableMap, variableExpressionExecutors,
                    false, 0, ProcessingMode.BATCH,
                    false, siddhiQueryContext);
        }
        return havingConditionExecutor;
    }

    public static ThreadLocal<String> getContainsAggregatorThreadLocal() {
        return containsAggregatorThreadLocal;
    }
}
