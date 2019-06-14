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

import io.siddhi.core.table.record.ExpressionVisitor;
import io.siddhi.query.api.expression.AttributeFunction;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.Variable;
import io.siddhi.query.api.expression.condition.And;
import io.siddhi.query.api.expression.condition.Compare;
import io.siddhi.query.api.expression.condition.In;
import io.siddhi.query.api.expression.condition.IsNull;
import io.siddhi.query.api.expression.condition.Not;
import io.siddhi.query.api.expression.condition.Or;
import io.siddhi.query.api.expression.constant.Constant;
import io.siddhi.query.api.expression.math.Add;
import io.siddhi.query.api.expression.math.Divide;
import io.siddhi.query.api.expression.math.Mod;
import io.siddhi.query.api.expression.math.Multiply;
import io.siddhi.query.api.expression.math.Subtract;

/**
 * Builder to the Aggregation on condition.
 */
public class AggregationExpressionBuilder {

    private Expression expression;

    public AggregationExpressionBuilder(Expression expression) {
        this.expression = expression;
    }

    public void build(AggregationExpressionVisitor expressionVisitor) {
        build(expression, expressionVisitor);
    }

    private void build(Expression expression, AggregationExpressionVisitor expressionVisitor) {
        if (expression instanceof And) {
            build(((And) expression).getLeftExpression(), expressionVisitor);
            build(((And) expression).getRightExpression(), expressionVisitor);
            expressionVisitor.endVisitAnd();
        } else if (expression instanceof Or) {
            build(((Or) expression).getLeftExpression(), expressionVisitor);
            build(((Or) expression).getRightExpression(), expressionVisitor);
            expressionVisitor.endVisitOr();
        } else if (expression instanceof Not) {
            build(((Not) expression).getExpression(), expressionVisitor);
            expressionVisitor.endVisitNot();
        } else if (expression instanceof Compare) {
            build(((Compare) expression).getLeftExpression(), expressionVisitor);
            build(((Compare) expression).getRightExpression(), expressionVisitor);
            expressionVisitor.endVisitCompare(((Compare) expression).getOperator());
        } else if (expression instanceof Constant) {
            expressionVisitor.addConstantExpression(expression);
        } else if (expression instanceof Variable) {
            expressionVisitor.addVariableExpression(expression);
        } else if (expression instanceof IsNull) {
            IsNull isNull = (IsNull) expression;
            if (isNull.getExpression() != null) {
                build(((IsNull) expression).getExpression(), expressionVisitor);
                expressionVisitor.endVisitIsNull(null);
            }
        } else if (expression instanceof AttributeFunction) {

            Expression[] expressions = ((AttributeFunction) expression).getParameters();
            for (Expression value : expressions) {
                build(value, expressionVisitor);
            }
            expressionVisitor.endVisitAttributeFunction(
                    ((AttributeFunction) expression).getNamespace(),
                    ((AttributeFunction) expression).getName(),
                    expressions.length);

        } else if (expression instanceof Add) {
            build(((Add) expression).getLeftValue(), expressionVisitor);
            build(((Add) expression).getRightValue(), expressionVisitor);
            expressionVisitor.endVisitMath(ExpressionVisitor.MathOperator.ADD);
        } else if (expression instanceof Subtract) {
            build(((Subtract) expression).getLeftValue(), expressionVisitor);
            build(((Subtract) expression).getRightValue(), expressionVisitor);
            expressionVisitor.endVisitMath(ExpressionVisitor.MathOperator.SUBTRACT);

        } else if (expression instanceof Divide) {
            build(((Divide) expression).getLeftValue(), expressionVisitor);
            build(((Divide) expression).getRightValue(), expressionVisitor);
            expressionVisitor.endVisitMath(ExpressionVisitor.MathOperator.DIVIDE);
        } else if (expression instanceof Multiply) {
            build(((Multiply) expression).getLeftValue(), expressionVisitor);
            build(((Multiply) expression).getRightValue(), expressionVisitor);
            expressionVisitor.endVisitMath(ExpressionVisitor.MathOperator.MULTIPLY);
        } else if (expression instanceof Mod) {
            build(((Mod) expression).getLeftValue(), expressionVisitor);
            build(((Mod) expression).getRightValue(), expressionVisitor);
            expressionVisitor.endVisitMath(ExpressionVisitor.MathOperator.MOD);
        } else if (expression instanceof In) {
            build(((In) expression).getExpression(), expressionVisitor);
            expressionVisitor.endVisitIn(((In) expression).getSourceId());
        }
    }
}
