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

package io.siddhi.core.util.collection.expression;

import io.siddhi.query.api.expression.Expression;

import java.util.HashSet;
import java.util.Set;

/**
 * Implementation of {@link CollectionExpression} which represent Null expressions.
 */
public class NullCollectionExpression implements CollectionExpression {

    private final Expression expression;
    private final CollectionScope collectionScope;
    private String attribute;

    public NullCollectionExpression(Expression expression, CollectionScope collectionScope, String attribute) {
        this.expression = expression;
        this.collectionScope = collectionScope;
        this.attribute = attribute;
    }

    public Expression getExpression() {
        return expression;
    }

    @Override
    public CollectionScope getCollectionScope() {
        return collectionScope;
    }

    @Override
    public Set<String> getMultiPrimaryKeys() {
        return new HashSet<>();
    }

    public String getAttribute() {
        return attribute;
    }
}
