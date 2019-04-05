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
package io.siddhi.core.query.selector;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.MetaComplexEvent;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.parser.ExpressionParser;
import io.siddhi.query.api.expression.Expression;

import java.util.List;
import java.util.Map;

/**
 * Class to generate keys for GroupBy groups
 */
public class GroupByKeyGenerator {

    private ExpressionExecutor[] groupByExecutors = null;

    public GroupByKeyGenerator(List<Expression> groupByList,
                               MetaComplexEvent metaComplexEvent,
                               int currentState, Map<String, Table> tableMap,
                               List<VariableExpressionExecutor> executors,
                               SiddhiQueryContext siddhiQueryContext) {
        if (!groupByList.isEmpty()) {
            groupByExecutors = new ExpressionExecutor[groupByList.size()];
            for (int i = 0, expressionsSize = groupByList.size(); i < expressionsSize; i++) {
                groupByExecutors[i] = ExpressionParser.parseExpression(
                        groupByList.get(i), metaComplexEvent, currentState, tableMap, executors,
                        false, 0, ProcessingMode.BATCH, false,
                        siddhiQueryContext);
            }
        }
    }

    /**
     * generate groupBy key of a streamEvent
     *
     * @param event complexEvent
     * @return GroupByKey
     */
    public String constructEventKey(ComplexEvent event) {
        if (groupByExecutors != null) {
            StringBuilder sb = new StringBuilder();
            for (ExpressionExecutor executor : groupByExecutors) {
                sb.append(executor.execute(event)).append(SiddhiConstants.KEY_DELIMITER);
            }
            return sb.toString();
        } else {
            return null;
        }
    }
}
