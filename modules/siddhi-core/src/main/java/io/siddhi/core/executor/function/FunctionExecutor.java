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
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.extension.validator.InputParameterValidator;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.snapshot.state.StateHolder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Parent abstract class for Function Executors. Function executor will have one or more input parameters and single
 * return value.
 *
 * @param <S> current state for the Function Executor
 */
public abstract class FunctionExecutor<S extends State> implements ExpressionExecutor {

    private static final Logger log = LogManager.getLogger(FunctionExecutor.class);
    protected ExpressionExecutor[] attributeExpressionExecutors;
    protected SiddhiQueryContext siddhiQueryContext;
    protected String functionId;
    protected ProcessingMode processingMode;
    private ConfigReader configReader;
    private int attributeSize;
    private StateHolder<S> stateHolder;

    public void initExecutor(ExpressionExecutor[] attributeExpressionExecutors,
                             ProcessingMode processingMode, ConfigReader configReader,
                             boolean groupBy, SiddhiQueryContext siddhiQueryContext) {
        this.processingMode = processingMode;
        this.configReader = configReader;
        this.siddhiQueryContext = siddhiQueryContext;
        try {
            this.attributeExpressionExecutors = attributeExpressionExecutors;
            attributeSize = attributeExpressionExecutors.length;
            InputParameterValidator.validateExpressionExecutors(this, attributeExpressionExecutors);
            StateFactory<S> stateFactory = init(attributeExpressionExecutors, configReader, this.siddhiQueryContext);
            stateHolder = this.siddhiQueryContext.generateStateHolder(this.getClass().getName(), groupBy, stateFactory);
        } catch (Throwable t) {
            throw new SiddhiAppCreationException(t.getMessage(), t);
        }
    }

    /**
     * The initialization method for FunctionExecutor, this method will be called before the other methods
     *
     * @param attributeExpressionExecutors are the executors of each function parameters
     * @param configReader                 This hold the {@link FunctionExecutor} extensions configuration reader.
     * @param siddhiQueryContext           the context of the siddhi query
     */
    protected abstract StateFactory<S> init(ExpressionExecutor[] attributeExpressionExecutors,
                                            ConfigReader configReader,
                                            SiddhiQueryContext siddhiQueryContext);

    /**
     * The main execution method which will be called upon event arrival
     *
     * @param event the event to be executed
     * @return the execution result
     */
    @Override
    public Object execute(ComplexEvent event) {
        try {
            S state = stateHolder.getState();
            try {
                switch (attributeSize) {
                    case 0:
                        return execute((Object) null, state);
                    case 1:
                        return execute(attributeExpressionExecutors[0].execute(event), state);
                    default:
                        Object[] data = new Object[attributeSize];
                        for (int i = 0; i < attributeSize; i++) {
                            data[i] = attributeExpressionExecutors[i].execute(event);
                        }
                        return execute(data, state);
                }
            } finally {
                stateHolder.returnState(state);
            }
        } catch (Exception e) {
            throw new SiddhiAppRuntimeException(e.getMessage() + ". Exception on class '" + this.getClass().getName()
                    + "'", e);
        }
    }

    /**
     * The main execution method which will be called upon event arrival
     * when there are more then one function parameter
     *
     * @param data the runtime values of function parameters
     * @return the function result
     */
    protected Object execute(Object[] data) {
        S state = stateHolder.getState();
        try {
            return execute(data, state);
        } finally {
            stateHolder.returnState(state);
        }
    }

    /**
     * The main execution method which will be called upon event arrival
     * when there are zero or one function parameter
     *
     * @param data null if the function parameter count is zero or
     *             runtime data value of the function parameter
     * @return the function result
     */
    protected Object execute(Object data) {
        S state = stateHolder.getState();
        try {
            return execute(data, state);
        } finally {
            stateHolder.returnState(state);
        }
    }

    /**
     * The main execution method which will be called upon event arrival
     * when there are more then one function parameter
     *
     * @param data  the runtime values of function parameters
     * @param state current query state
     * @return the function result
     */
    protected abstract Object execute(Object[] data, S state);

    /**
     * The main execution method which will be called upon event arrival
     * when there are zero or one function parameter
     *
     * @param data  null if the function parameter count is zero or
     *              runtime data value of the function parameter
     * @param state current query state
     * @return the function result
     */
    protected abstract Object execute(Object data, S state);
}
