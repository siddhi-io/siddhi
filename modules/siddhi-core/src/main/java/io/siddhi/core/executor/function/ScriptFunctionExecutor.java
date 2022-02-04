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

package io.siddhi.core.executor.function;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.function.Script;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.definition.Attribute;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Executor class for Script function. Function execution logic is implemented in execute here.
 */
public class ScriptFunctionExecutor extends FunctionExecutor {

    static final Logger LOG = LogManager.getLogger(ScriptFunctionExecutor.class);
    Attribute.Type returnType;
    Script script;

    public ScriptFunctionExecutor() {
    }

    public ScriptFunctionExecutor(String name) {
        this.functionId = name;
    }

    @Override
    public Attribute.Type getReturnType() {
        return returnType;
    }

    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
        returnType = siddhiQueryContext.getSiddhiAppContext().getScript(functionId).getReturnType();
        script = siddhiQueryContext.getSiddhiAppContext().getScript(functionId);
        return null;
    }

    @Override
    protected Object execute(Object[] data, State state) {
        return script.eval(functionId, data);
    }

    @Override
    protected Object execute(Object data, State state) {
        return script.eval(functionId, new Object[]{data});
    }

}
