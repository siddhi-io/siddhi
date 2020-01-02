/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.core.table.record;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.state.MetaStateEvent;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.exception.OperationNotSupportedException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.ExceptionUtil;
import io.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import io.siddhi.core.util.parser.ExpressionParser;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.AttributeNotExistException;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import io.siddhi.query.api.expression.AttributeFunction;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.Variable;
import io.siddhi.query.api.expression.condition.And;
import io.siddhi.query.api.expression.condition.Compare;
import io.siddhi.query.api.expression.condition.In;
import io.siddhi.query.api.expression.condition.IsNull;
import io.siddhi.query.api.expression.condition.Not;
import io.siddhi.query.api.expression.condition.Or;
import io.siddhi.query.api.expression.constant.BoolConstant;
import io.siddhi.query.api.expression.constant.Constant;
import io.siddhi.query.api.expression.constant.DoubleConstant;
import io.siddhi.query.api.expression.constant.FloatConstant;
import io.siddhi.query.api.expression.constant.IntConstant;
import io.siddhi.query.api.expression.constant.LongConstant;
import io.siddhi.query.api.expression.constant.StringConstant;
import io.siddhi.query.api.expression.math.Add;
import io.siddhi.query.api.expression.math.Divide;
import io.siddhi.query.api.expression.math.Mod;
import io.siddhi.query.api.expression.math.Multiply;
import io.siddhi.query.api.expression.math.Subtract;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.siddhi.core.util.SiddhiConstants.UNKNOWN_STATE;

/**
 * Parse and build Siddhi Condition objects from @{link {@link Expression}s.
 */
public class ExpressionBuilder {
    private final Map<String, ExpressionExecutor> variableExpressionExecutorMap;
    private final MatchingMetaInfoHolder matchingMetaInfoHolder;
    private final UpdateOrInsertReducer updateOrInsertReducer;
    private final SiddhiQueryContext siddhiQueryContext;
    private final List<VariableExpressionExecutor> variableExpressionExecutors;
    private final Map<String, Table> tableMap;
    private final Expression expression;
    private ExpressionExecutor inMemorySetExpressionExecutor;

    ExpressionBuilder(Expression expression, MatchingMetaInfoHolder matchingMetaInfoHolder,
                      List<VariableExpressionExecutor> variableExpressionExecutors, Map<String, Table> tableMap,
                      UpdateOrInsertReducer updateOrInsertReducer, ExpressionExecutor inMemorySetExpressionExecutor,
                      SiddhiQueryContext siddhiQueryContext) {
        this.expression = expression;
        this.matchingMetaInfoHolder = matchingMetaInfoHolder;
        this.updateOrInsertReducer = updateOrInsertReducer;
        this.inMemorySetExpressionExecutor = inMemorySetExpressionExecutor;
        this.siddhiQueryContext = siddhiQueryContext;
        this.variableExpressionExecutors = variableExpressionExecutors;
        this.tableMap = tableMap;
        this.variableExpressionExecutorMap = new HashMap<String, ExpressionExecutor>();
    }

    public MatchingMetaInfoHolder getMatchingMetaInfoHolder() {
        return matchingMetaInfoHolder;
    }

    public SiddhiQueryContext getSiddhiQueryContext() {
        return siddhiQueryContext;
    }

    public List<VariableExpressionExecutor> getVariableExpressionExecutors() {
        return variableExpressionExecutors;
    }

    public Map<String, Table> getTableMap() {
        return tableMap;
    }

    public Expression getExpression() {
        return expression;
    }

    public UpdateOrInsertReducer getUpdateOrInsertReducer() {
        return updateOrInsertReducer;
    }

    public ExpressionExecutor getInMemorySetExpressionExecutor() {
        return inMemorySetExpressionExecutor;
    }

    Map<String, ExpressionExecutor> getVariableExpressionExecutorMap() {
        return variableExpressionExecutorMap;
    }

    public void build(ExpressionVisitor expressionVisitor) {
        buildVariableExecutors(expression, expressionVisitor);
    }

    private void buildVariableExecutors(Expression expression, ExpressionVisitor expressionVisitor) {
        try {
            if (expression instanceof And) {
                expressionVisitor.beginVisitAnd();
                expressionVisitor.beginVisitAndLeftOperand();
                buildVariableExecutors(((And) expression).getLeftExpression(), expressionVisitor);
                expressionVisitor.endVisitAndLeftOperand();

                expressionVisitor.beginVisitAndRightOperand();
                buildVariableExecutors(((And) expression).getRightExpression(), expressionVisitor);
                expressionVisitor.endVisitAndRightOperand();
                expressionVisitor.endVisitAnd();

            } else if (expression instanceof Or) {
                expressionVisitor.beginVisitOr();
                expressionVisitor.beginVisitOrLeftOperand();
                buildVariableExecutors(((Or) expression).getLeftExpression(), expressionVisitor);
                expressionVisitor.endVisitOrLeftOperand();

                expressionVisitor.beginVisitOrRightOperand();
                buildVariableExecutors(((Or) expression).getRightExpression(), expressionVisitor);
                expressionVisitor.endVisitOrRightOperand();
                expressionVisitor.endVisitOr();

            } else if (expression instanceof Not) {
                expressionVisitor.beginVisitNot();
                buildVariableExecutors(((Not) expression).getExpression(), expressionVisitor);
                expressionVisitor.endVisitNot();

            } else if (expression instanceof Compare) {
                expressionVisitor.beginVisitCompare(((Compare) expression).getOperator());
                expressionVisitor.beginVisitCompareLeftOperand(((Compare) expression).getOperator());
                buildVariableExecutors(((Compare) expression).getLeftExpression(), expressionVisitor);
                expressionVisitor.endVisitCompareLeftOperand(((Compare) expression).getOperator());

                expressionVisitor.beginVisitCompareRightOperand(((Compare) expression).getOperator());
                buildVariableExecutors(((Compare) expression).getRightExpression(), expressionVisitor);
                expressionVisitor.endVisitCompareRightOperand(((Compare) expression).getOperator());
                expressionVisitor.endVisitCompare(((Compare) expression).getOperator());

            } else if (expression instanceof Add) {
                expressionVisitor.beginVisitMath(ExpressionVisitor.MathOperator.ADD);
                expressionVisitor.beginVisitMathLeftOperand(ExpressionVisitor.MathOperator.ADD);
                buildVariableExecutors(((Add) expression).getLeftValue(), expressionVisitor);
                expressionVisitor.endVisitMathLeftOperand(ExpressionVisitor.MathOperator.ADD);

                expressionVisitor.beginVisitMathRightOperand(ExpressionVisitor.MathOperator.ADD);
                buildVariableExecutors(((Add) expression).getRightValue(), expressionVisitor);
                expressionVisitor.endVisitMathRightOperand(ExpressionVisitor.MathOperator.ADD);
                expressionVisitor.endVisitMath(ExpressionVisitor.MathOperator.ADD);
            } else if (expression instanceof Subtract) {
                expressionVisitor.beginVisitMath(ExpressionVisitor.MathOperator.SUBTRACT);
                expressionVisitor.beginVisitMathLeftOperand(ExpressionVisitor.MathOperator.SUBTRACT);
                buildVariableExecutors(((Subtract) expression).getLeftValue(), expressionVisitor);
                expressionVisitor.endVisitMathLeftOperand(ExpressionVisitor.MathOperator.SUBTRACT);

                expressionVisitor.beginVisitMathRightOperand(ExpressionVisitor.MathOperator.SUBTRACT);
                buildVariableExecutors(((Subtract) expression).getRightValue(), expressionVisitor);
                expressionVisitor.endVisitMathRightOperand(ExpressionVisitor.MathOperator.SUBTRACT);
                expressionVisitor.endVisitMath(ExpressionVisitor.MathOperator.SUBTRACT);

            } else if (expression instanceof Divide) {
                expressionVisitor.beginVisitMath(ExpressionVisitor.MathOperator.DIVIDE);
                expressionVisitor.beginVisitMathLeftOperand(ExpressionVisitor.MathOperator.DIVIDE);
                buildVariableExecutors(((Divide) expression).getLeftValue(), expressionVisitor);
                expressionVisitor.endVisitMathLeftOperand(ExpressionVisitor.MathOperator.DIVIDE);

                expressionVisitor.beginVisitMathRightOperand(ExpressionVisitor.MathOperator.DIVIDE);
                buildVariableExecutors(((Divide) expression).getRightValue(), expressionVisitor);
                expressionVisitor.endVisitMathRightOperand(ExpressionVisitor.MathOperator.DIVIDE);
                expressionVisitor.endVisitMath(ExpressionVisitor.MathOperator.DIVIDE);
            } else if (expression instanceof Multiply) {
                expressionVisitor.beginVisitMath(ExpressionVisitor.MathOperator.MULTIPLY);
                expressionVisitor.beginVisitMathLeftOperand(ExpressionVisitor.MathOperator.MULTIPLY);
                buildVariableExecutors(((Multiply) expression).getLeftValue(), expressionVisitor);
                expressionVisitor.endVisitMathLeftOperand(ExpressionVisitor.MathOperator.MULTIPLY);

                expressionVisitor.beginVisitMathRightOperand(ExpressionVisitor.MathOperator.MULTIPLY);
                buildVariableExecutors(((Multiply) expression).getRightValue(), expressionVisitor);
                expressionVisitor.endVisitMathRightOperand(ExpressionVisitor.MathOperator.MULTIPLY);
                expressionVisitor.endVisitMath(ExpressionVisitor.MathOperator.MULTIPLY);

            } else if (expression instanceof Mod) {
                expressionVisitor.beginVisitMath(ExpressionVisitor.MathOperator.MOD);
                expressionVisitor.beginVisitMathLeftOperand(ExpressionVisitor.MathOperator.MOD);
                buildVariableExecutors(((Mod) expression).getLeftValue(), expressionVisitor);
                expressionVisitor.endVisitMathLeftOperand(ExpressionVisitor.MathOperator.MOD);

                expressionVisitor.beginVisitMathRightOperand(ExpressionVisitor.MathOperator.MOD);
                buildVariableExecutors(((Mod) expression).getRightValue(), expressionVisitor);
                expressionVisitor.endVisitMathRightOperand(ExpressionVisitor.MathOperator.MOD);
                expressionVisitor.endVisitMath(ExpressionVisitor.MathOperator.MOD);

            } else if (expression instanceof IsNull) {
                IsNull isNull = (IsNull) expression;
                if (isNull.getExpression() != null) {
                    expressionVisitor.beginVisitIsNull(null);
                    buildVariableExecutors(((IsNull) expression).getExpression(), expressionVisitor);
                    expressionVisitor.endVisitIsNull(null);
                } else {
                    String streamId = isNull.getStreamId();
                    MetaStateEvent metaStateEvent = matchingMetaInfoHolder.getMetaStateEvent();
                    if (streamId == null) {
                        throw new SiddhiAppCreationException("IsNull does not support streamId being null");
                    } else {
                        AbstractDefinition definitionOutput = null;
                        MetaStreamEvent[] metaStreamEvents = metaStateEvent.getMetaStreamEvents();
                        for (int i = 0, metaStreamEventsLength = metaStreamEvents.length;
                             i < metaStreamEventsLength; i++) {
                            MetaStreamEvent metaStreamEvent = metaStreamEvents[i];
                            AbstractDefinition definition = metaStreamEvent.getLastInputDefinition();
                            if (metaStreamEvent.getInputReferenceId() == null) {
                                if (definition.getId().equals(streamId)) {
                                    definitionOutput = definition;
                                    break;
                                }
                            } else {
                                if (metaStreamEvent.getInputReferenceId().equals(streamId)) {
                                    definitionOutput = definition;
                                    break;
                                }
                            }
                        }
                        if (definitionOutput != null) {
                            expressionVisitor.beginVisitIsNull(definitionOutput.getId());
                            expressionVisitor.endVisitIsNull(definitionOutput.getId());
                        } else {
                            expressionVisitor.beginVisitIsNull(null);
                            expressionVisitor.endVisitIsNull(null);
                        }
                    }
                }
            } else if (expression instanceof In) {
                expressionVisitor.beginVisitIn(((In) expression).getSourceId());
                buildVariableExecutors(((In) expression).getExpression(), expressionVisitor);
                expressionVisitor.endVisitIn(((In) expression).getSourceId());

            } else if (expression instanceof Constant) {
                if (expression instanceof DoubleConstant) {
                    expressionVisitor.beginVisitConstant(((DoubleConstant) expression).getValue(),
                            Attribute.Type.DOUBLE);
                    expressionVisitor.endVisitConstant(((DoubleConstant) expression).getValue(),
                            Attribute.Type.DOUBLE);
                } else if (expression instanceof StringConstant) {
                    expressionVisitor.beginVisitConstant(((StringConstant) expression).getValue(),
                            Attribute.Type.STRING);
                    expressionVisitor.endVisitConstant(((StringConstant) expression).getValue(),
                            Attribute.Type.STRING);
                } else if (expression instanceof IntConstant) {
                    expressionVisitor.beginVisitConstant(((IntConstant) expression).getValue(), Attribute.Type.INT);
                    expressionVisitor.endVisitConstant(((IntConstant) expression).getValue(), Attribute.Type.INT);
                } else if (expression instanceof BoolConstant) {
                    expressionVisitor.beginVisitConstant(((BoolConstant) expression).getValue(), Attribute.Type.BOOL);
                    expressionVisitor.endVisitConstant(((BoolConstant) expression).getValue(), Attribute.Type.BOOL);
                } else if (expression instanceof FloatConstant) {
                    expressionVisitor.beginVisitConstant(((FloatConstant) expression).getValue(),
                            Attribute.Type.FLOAT);
                    expressionVisitor.endVisitConstant(((FloatConstant) expression).getValue(), Attribute.Type.FLOAT);
                } else if (expression instanceof LongConstant) {
                    expressionVisitor.beginVisitConstant(((LongConstant) expression).getValue(), Attribute.Type.LONG);
                    expressionVisitor.endVisitConstant(((LongConstant) expression).getValue(), Attribute.Type.LONG);
                } else {
                    throw new OperationNotSupportedException("No constant exist with type " +
                            expression.getClass().getName());
                }
            } else if (expression instanceof AttributeFunction) {
                expressionVisitor.beginVisitAttributeFunction(
                        ((AttributeFunction) expression).getNamespace(),
                        ((AttributeFunction) expression).getName());
                Expression[] expressions = ((AttributeFunction) expression).getParameters();
                if (expressions != null) {
                    for (int i = 0; i < expressions.length; i++) {
                        expressionVisitor.beginVisitParameterAttributeFunction(i);
                        buildVariableExecutors(expressions[i], expressionVisitor);
                        expressionVisitor.endVisitParameterAttributeFunction(i);
                    }
                }
                expressionVisitor.endVisitAttributeFunction(
                        ((AttributeFunction) expression).getNamespace(),
                        ((AttributeFunction) expression).getName());

            } else if (expression instanceof Variable) {
                Variable variable = ((Variable) expression);
                String attributeName = variable.getAttributeName();
                AbstractDefinition definition;
                Attribute.Type type = null;
                int streamEventChainIndex = matchingMetaInfoHolder.getCurrentState();

                if (variable.getStreamId() == null) {
                    MetaStreamEvent[] metaStreamEvents = matchingMetaInfoHolder.getMetaStateEvent().
                            getMetaStreamEvents();

                    if (streamEventChainIndex == UNKNOWN_STATE) {
                        String firstInput = null;
                        for (int i = 0; i < metaStreamEvents.length; i++) {
                            MetaStreamEvent metaStreamEvent = metaStreamEvents[i];
                            definition = metaStreamEvent.getLastInputDefinition();
                            if (type == null) {
                                try {
                                    type = definition.getAttributeType(attributeName);
                                    firstInput = "Input Stream: " + definition.getId() + " with " +
                                            "reference: " + metaStreamEvent.getInputReferenceId();
                                    streamEventChainIndex = i;
                                } catch (AttributeNotExistException e) {
                                    //do nothing
                                }
                            } else {
                                try {
                                    definition.getAttributeType(attributeName);
                                    throw new SiddhiAppValidationException(firstInput + " and Input Stream: " +
                                            definition.getId() + " with " +
                                            "reference: " + metaStreamEvent
                                            .getInputReferenceId() + " contains attribute " +
                                            "with same" +
                                            " name '" + attributeName + "'");
                                } catch (AttributeNotExistException e) {
                                    //do nothing as its expected
                                }
                            }
                        }
                        if (streamEventChainIndex != UNKNOWN_STATE) {
                            if (matchingMetaInfoHolder.getMatchingStreamEventIndex() == streamEventChainIndex) {
                                buildStreamVariableExecutor(variable, streamEventChainIndex, expressionVisitor, type);
                            } else {
                                buildStoreVariableExecutor(variable, expressionVisitor, type, matchingMetaInfoHolder
                                        .getStoreDefinition());
                            }
                        } else {
                            // Having state : i.e attribute is in the select clause
                            definition = matchingMetaInfoHolder.getMetaStateEvent().getOutputStreamDefinition();
                            try {
                                type = definition.getAttributeType(attributeName);
                                buildStoreVariableExecutor(variable, expressionVisitor, type, matchingMetaInfoHolder
                                        .getStoreDefinition());
                            } catch (AttributeNotExistException e) {
                                //do nothing as its not expected
                            }
                        }
                    } else {

                        MetaStreamEvent metaStreamEvent = matchingMetaInfoHolder.getMetaStateEvent().getMetaStreamEvent
                                (matchingMetaInfoHolder.getCurrentState());
                        definition = metaStreamEvent.getLastInputDefinition();
                        try {
                            type = definition.getAttributeType(attributeName);
                        } catch (AttributeNotExistException e) {
                            // Having state : i.e attribute is in the select clause
                            definition = matchingMetaInfoHolder.getMetaStateEvent().getOutputStreamDefinition();
                            try {
                                type = definition.getAttributeType(attributeName);
                                buildStoreVariableExecutor(variable, expressionVisitor, type, matchingMetaInfoHolder
                                        .getStoreDefinition());
                            } catch (AttributeNotExistException e1) {
                                throw new SiddhiAppValidationException(e1.getMessageWithOutContext() +
                                        " Input Stream: " + definition.getId() + " with " + "reference: " +
                                        metaStreamEvent.getInputReferenceId(), e1.getQueryContextStartIndex(),
                                        e1.getQueryContextEndIndex(),
                                        siddhiQueryContext.getSiddhiAppContext().getName(),
                                        siddhiQueryContext.getSiddhiAppContext().getSiddhiAppString());
                            }
                        }

                        if (matchingMetaInfoHolder.getCurrentState() == matchingMetaInfoHolder
                                .getMatchingStreamEventIndex()) {
                            buildStreamVariableExecutor(variable, streamEventChainIndex, expressionVisitor, type);
                        } else {
                            buildStoreVariableExecutor(variable, expressionVisitor, type, matchingMetaInfoHolder
                                    .getStoreDefinition());
                        }
                    }

                } else {

                    MetaStreamEvent[] metaStreamEvents = matchingMetaInfoHolder.getMetaStateEvent().
                            getMetaStreamEvents();
                    for (int i = 0, metaStreamEventsLength = metaStreamEvents.length; i < metaStreamEventsLength; i++) {
                        MetaStreamEvent metaStreamEvent = metaStreamEvents[i];
                        definition = metaStreamEvent.getLastInputDefinition();
                        if (metaStreamEvent.getInputReferenceId() == null) {
                            if (definition.getId().equals(variable.getStreamId())) {
                                type = definition.getAttributeType(attributeName);
                                streamEventChainIndex = i;
                                break;
                            }
                        } else {
                            if (metaStreamEvent.getInputReferenceId().equals(variable.getStreamId())) {
                                type = definition.getAttributeType(attributeName);
                                streamEventChainIndex = i;
                                break;
                            }
                        }
                    }
                    if (matchingMetaInfoHolder.getMatchingStreamEventIndex() == streamEventChainIndex) {
                        buildStreamVariableExecutor(variable, streamEventChainIndex, expressionVisitor, type);
                    } else {
                        buildStoreVariableExecutor(variable, expressionVisitor, type, matchingMetaInfoHolder
                                .getStoreDefinition());
                    }
                }
            }
        } catch (Throwable t) {
            ExceptionUtil.populateQueryContext(t, expression, siddhiQueryContext.getSiddhiAppContext(),
                    siddhiQueryContext);
            throw t;
        }
    }

    private void buildStoreVariableExecutor(Variable variable, ExpressionVisitor expressionVisitor, Attribute.Type type,
                                            AbstractDefinition storeDefinition) {
        expressionVisitor.beginVisitStoreVariable(storeDefinition.getId(), variable.getAttributeName(), type);
        expressionVisitor.endVisitStoreVariable(storeDefinition.getId(), variable.getAttributeName(), type);

    }

    private void buildStreamVariableExecutor(Variable variable, int streamEventChainIndex,
                                             ExpressionVisitor expressionVisitor, Attribute.Type type) {
        String id = variable.getAttributeName();
        if (variable.getStreamId() != null) {
            id = variable.getStreamId() + "." + id;
        }
        expressionVisitor.beginVisitStreamVariable(id, variable.getStreamId(), variable.getAttributeName(), type);
        if (!variableExpressionExecutorMap.containsKey(id)) {
            ExpressionExecutor variableExpressionExecutor = ExpressionParser.parseExpression(
                    variable, matchingMetaInfoHolder.getMetaStateEvent(), streamEventChainIndex, tableMap,
                    variableExpressionExecutors, false, 0,
                    ProcessingMode.BATCH, false, siddhiQueryContext);
            variableExpressionExecutorMap.put(id, variableExpressionExecutor);
        }
        expressionVisitor.endVisitStreamVariable(id, variable.getStreamId(), variable.getAttributeName(), type);

    }
}
