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
package io.siddhi.query.api.execution.query.selection;

import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.Variable;

import java.util.List;

/**
 * Basic Selector API
 */
public class BasicSelector extends Selector {

    private static final long serialVersionUID = 1L;

    public BasicSelector select(String rename, Expression expression) {

        return (BasicSelector) super.select(rename, expression);
    }

    public BasicSelector select(Variable variable) {

        return (BasicSelector) super.select(variable);

    }

    public BasicSelector groupBy(Variable variable) {

        return (BasicSelector) super.groupBy(variable);

    }

    public BasicSelector addGroupByList(List<Variable> list) {

        return (BasicSelector) super.addGroupByList(list);

    }

    public BasicSelector addSelectionList(List<OutputAttribute> projectionList) {

        return (BasicSelector) super.addSelectionList(projectionList);

    }

}
