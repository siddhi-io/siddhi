/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.query.api.definition;


import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.util.SiddhiConstants;

/**
 * Siddhi execution plan level variable definition.
 */
public class ExpressionDefinition extends AbstractDefinition {

    private static final long serialVersionUID = 1L;

    private String id;
    private Expression expression;

    protected ExpressionDefinition(String name) {
        this.id = SiddhiConstants.GLOBAL_EXPRESSION_PREFIX + name;
    }

    public static ExpressionDefinition name(String name) {
        return new ExpressionDefinition(name);
    }

    public String getId() {
        return id;
    }

    public Expression getExpression() {
        return expression;
    }

    public ExpressionDefinition expression(Expression expression) {
        this.expression = expression;
        return this;
    }

    @Override
    public String toString() {
        return "Variable{" +
                "id='" + id + '\'' +
                ", expression=" + expression +
                '}';
    }
}
