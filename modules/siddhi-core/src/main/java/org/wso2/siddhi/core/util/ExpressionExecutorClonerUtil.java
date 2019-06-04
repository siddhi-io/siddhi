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

package org.wso2.siddhi.core.util;

import org.wso2.siddhi.core.executor.ExpressionExecutor;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Util class to clone Expression Executors
 */
public class ExpressionExecutorClonerUtil {

    public static List<ExpressionExecutor> getExpressionExecutorClones(
                                                                    List<ExpressionExecutor> expressionExecutorList) {
        return expressionExecutorList.stream()
                .map(expressionExecutor -> expressionExecutor.cloneExecutor(""))
                .collect(Collectors.toList());
    }

    public static ExpressionExecutor getExpressionExecutorClone(ExpressionExecutor expressionExecutor) {
        if (expressionExecutor != null) {
            return expressionExecutor.cloneExecutor("");
        }
        return null;
    }

}
