/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.core.aggregation;

import io.siddhi.core.table.record.BaseExpressionVisitor;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.Variable;
import io.siddhi.query.api.expression.condition.Compare;

import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;

/**
 * Visitor class to reduce on condition Expression
 */
public class AggregationExpressionVisitor extends BaseExpressionVisitor {

    private Stack<Object> conditionOperands;
    private String inputStreamRefId;
    private List<String> tableAttributesNameList;
    private List<String> allAttributesList;


    AggregationExpressionVisitor(String inputStreamRefId,
                                 List<Attribute> inputStreamAttributesList, List<String> tableAttributesNameList) {
        this.conditionOperands = new Stack<>();
        this.inputStreamRefId = inputStreamRefId;
        this.tableAttributesNameList = tableAttributesNameList;
        this.allAttributesList = inputStreamAttributesList.stream()
                .map(Attribute::getName)
                .collect(Collectors.toList());
        this.allAttributesList.addAll(tableAttributesNameList);
    }

    public boolean applyReducedExpression() {
        Object peek = this.conditionOperands.peek();
        if (peek instanceof String) {
            return false;
        }
        return true;
    }

    public Expression getReducedExpression() {
        Object pop = this.conditionOperands.pop();
        if (pop instanceof String) {
            return Expression.value(true);
        }
        return ((Expression) pop);
    }

    @Override
    public void endVisitAnd() {
        Object rightOperand = this.conditionOperands.pop();
        Object leftOperand = this.conditionOperands.pop();

        boolean isLeftOperandString = leftOperand instanceof String;
        if (isLeftOperandString) {
            if (rightOperand instanceof String) {
                this.conditionOperands.push("true");
            } else {
                this.conditionOperands.push(rightOperand);
            }
        } else {
            if (rightOperand instanceof String) {
                this.conditionOperands.push(leftOperand);
            } else {
                this.conditionOperands.push(
                        Expression.and(
                                ((Expression) leftOperand), ((Expression) rightOperand)
                        )
                );
            }
        }
    }

    @Override
    public void endVisitOr() {
        Object rightOperand = this.conditionOperands.pop();
        Object leftOperand = this.conditionOperands.pop();

        boolean isLeftOperandString = leftOperand instanceof String;
        if (isLeftOperandString) {
            if (rightOperand instanceof String) {
                this.conditionOperands.push("true");
            } else {
                this.conditionOperands.push(rightOperand);
            }
        } else {
            if (rightOperand instanceof String) {
                this.conditionOperands.push(leftOperand);
            } else {
                this.conditionOperands.push(
                        Expression.or(
                                ((Expression) leftOperand), ((Expression) rightOperand)
                        )
                );
            }
        }
    }

    @Override
    public void endVisitNot() {
        Object operand = this.conditionOperands.pop();
        if (operand instanceof String) {
            this.conditionOperands.push("true");
        } else {
            this.conditionOperands.push(Expression.not((Expression) operand));
        }
    }


    @Override
    public void endVisitCompare(Compare.Operator operator) {
        Object rightOperand = this.conditionOperands.pop();
        Object leftOperand = this.conditionOperands.pop();
        if (!(rightOperand instanceof String) && !(leftOperand instanceof String)) {
            this.conditionOperands.push(
                    Expression.compare(
                            ((Expression) leftOperand), operator, ((Expression) rightOperand)
                    )
            );
        } else {
            this.conditionOperands.push("true");
        }
    }


    @Override
    public void endVisitIsNull(String streamId) {
        Object operand = this.conditionOperands.pop();
        if (operand instanceof String) {
            this.conditionOperands.push("true");
        } else {
            this.conditionOperands.push(Expression.isNull((Expression) operand));
        }
    }

    @Override
    public void endVisitIn(String storeId) {
        Object operand = this.conditionOperands.pop();
        if (operand instanceof String) {
            this.conditionOperands.push("true");
        } else {
            this.conditionOperands.push(Expression.in((Expression) operand, storeId));
        }
    }

    @Override
    public void endVisitMath(MathOperator mathOperator) {
        Object rightOperand = this.conditionOperands.pop();
        Object leftOperand = this.conditionOperands.pop();
        if (!(rightOperand instanceof String) && !(leftOperand instanceof String)) {
            Expression expression = null;
            switch (mathOperator) {
                case ADD:
                    expression = Expression.add(
                            ((Expression) leftOperand), ((Expression) rightOperand)
                    );
                    break;
                case SUBTRACT:
                    expression = Expression.subtract(
                            ((Expression) leftOperand), ((Expression) rightOperand)
                    );
                    break;
                case MULTIPLY:
                    expression = Expression.multiply(
                            ((Expression) leftOperand), ((Expression) rightOperand)
                    );
                    break;
                case DIVIDE:
                    expression = Expression.divide(
                            ((Expression) leftOperand), ((Expression) rightOperand)
                    );
                    break;
                case MOD:
                    expression = Expression.mod(
                            ((Expression) leftOperand), ((Expression) rightOperand)
                    );
                    break;
                default:
                    this.conditionOperands.push("true");
            }
            this.conditionOperands.push(expression);
        } else {
            this.conditionOperands.push("true");
        }
    }

    public void endVisitAttributeFunction(String namespace, String functionName, int numAttributeOperand) {
        Expression[] expressions = new Expression[numAttributeOperand];
        boolean toReduce = false;
        for (int i = 0; i < numAttributeOperand; i++) {
            Object operand = this.conditionOperands.pop();
            if (operand instanceof String) {
                toReduce = true;
                break;
            } else {
                expressions[numAttributeOperand - 1 - i] = ((Expression) operand);
            }
        }
        if (!toReduce) {
            this.conditionOperands.push(Expression.function(namespace, functionName, expressions));
        } else {
            this.conditionOperands.push("true");
        }
    }

    public void addConstantExpression(Expression expression) {
        this.conditionOperands.push(expression);
    }

    public void addVariableExpression(Expression expression) {
        Variable variable = (Variable) expression;
        String streamId = variable.getStreamId();
        if (streamId == null) {
            if (this.allAttributesList.contains(variable.getAttributeName())) {
                this.conditionOperands.push(expression);
            } else {
                this.conditionOperands.push("true");
            }
        } else {
            if (streamId.equals(inputStreamRefId)) {
                this.conditionOperands.push(expression);
            } else if (this.tableAttributesNameList.contains(variable.getAttributeName())) {
                this.conditionOperands.push(expression);
            } else {
                this.conditionOperands.push("true");
            }
        }

    }

}
