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
package io.siddhi.core.query.selector.attribute.aggregator;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.extension.validator.InputParameterValidator;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.snapshot.state.StateHolder;

/**
 * Abstract parent class for attribute aggregators. Attribute aggregators are used to perform aggregate operations
 * such as count, average, etc.
 *
 * @param <S> current state for the Attribute Executor
 */
public abstract class AttributeAggregatorExecutor<S extends State> implements ExpressionExecutor {

    protected ExpressionExecutor[] attributeExpressionExecutors;
    private int attributeSize;
    private StateHolder<S> stateHolder;

    public void initAggregator(ExpressionExecutor[] attributeExpressionExecutors, ProcessingMode processingMode,
                               boolean outputExpectsExpiredEvents,
                               ConfigReader configReader, boolean groupBy, SiddhiQueryContext siddhiQueryContext) {
        try {
            this.attributeExpressionExecutors = attributeExpressionExecutors;
            this.attributeSize = attributeExpressionExecutors.length;
            InputParameterValidator.validateExpressionExecutors(this, attributeExpressionExecutors);
            StateFactory<S> stateFactory = init(attributeExpressionExecutors, processingMode,
                    outputExpectsExpiredEvents, configReader, siddhiQueryContext);
            stateHolder = siddhiQueryContext.generateStateHolder(this.getClass().getName(),
                    groupBy, stateFactory, true);
        } catch (Throwable t) {
            throw new SiddhiAppCreationException(t);
        }
    }

    public Object execute(ComplexEvent event) {
        if (attributeSize > 1) {
            return processAttributeArray(event);
        } else if (attributeSize == 1) {
            return processAttribute(event);
        } else {
            return processNoAttribute(event);
        }
    }

    private Object processAttributeArray(ComplexEvent event) {
        Object[] data = new Object[attributeSize];
        for (int i = 0; i < attributeSize; i++) {
            data[i] = attributeExpressionExecutors[i].execute(event);
        }
        switch (event.getType()) {
            case CURRENT:
                return processAdd(data);
            case EXPIRED:
                return processRemove(data);
            case RESET:
                return processReset();
        }
        return null;
    }

    private Object processAttribute(ComplexEvent event) {
        switch (event.getType()) {
            case CURRENT:
                return processAdd(attributeExpressionExecutors[0].execute(event));
            case EXPIRED:
                return processRemove(attributeExpressionExecutors[0].execute(event));
            case RESET:
                return processReset();
        }
        return null;
    }

    private Object processNoAttribute(ComplexEvent event) {
        switch (event.getType()) {
            case CURRENT:
                return processAdd(null);
            case EXPIRED:
                return processRemove(null);
            case RESET:
                return processReset();
        }
        return null;
    }

    private Object processAdd(Object data) {
        S state = stateHolder.getState();
        try {
            return processAdd(data, state);
        } finally {
            stateHolder.returnState(state);
        }
    }

    private Object processRemove(Object data) {
        S state = stateHolder.getState();
        try {
            return processRemove(data, state);
        } finally {
            stateHolder.returnState(state);
        }
    }

    private Object processAdd(Object[] data) {
        S state = stateHolder.getState();
        try {
            return processAdd(data, state);
        } finally {
            stateHolder.returnState(state);
        }
    }

    private Object processRemove(Object[] data) {
        S state = stateHolder.getState();
        try {
            return processRemove(data, state);
        } finally {
            stateHolder.returnState(state);
        }
    }

    private Object processReset() {
        S state = stateHolder.cleanGroupByStates();
        if (state != null) {
            return reset(state);
        }
        return null;
    }

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param processingMode               query processing mode
     * @param outputExpectsExpiredEvents   is expired events sent as output
     * @param configReader                 this hold the {@link AttributeAggregatorExecutor} extensions
     *                                     configuration reader.
     * @param siddhiQueryContext           Siddhi query runtime context
     */
    protected abstract StateFactory<S> init(ExpressionExecutor[] attributeExpressionExecutors,
                                            ProcessingMode processingMode,
                                            boolean outputExpectsExpiredEvents, ConfigReader configReader,
                                            SiddhiQueryContext siddhiQueryContext);

    public abstract Object processAdd(Object data, S state);

    public abstract Object processAdd(Object[] data, S state);

    public abstract Object processRemove(Object data, S state);

    public abstract Object processRemove(Object[] data, S state);

    public abstract Object reset(S state);


}
