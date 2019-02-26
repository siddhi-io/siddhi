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
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.query.api.definition.Attribute;

import java.util.Map;

/**
 * Abstract parent class for attribute aggregators. Attribute aggregators are used to perform aggregate operations
 * such as count, average, etc.
 */
public abstract class AttributeAggregator {

    protected ExpressionExecutor[] attributeExpressionExecutors;
    private int attributeSize;
    private ProcessingMode processingMode;
    private boolean outputExpectsExpiredEvents;
    private SiddhiQueryContext siddhiQueryContext;
    private ConfigReader configReader;

    public void initAggregator(ExpressionExecutor[] attributeExpressionExecutors, ProcessingMode processingMode,
                               boolean outputExpectsExpiredEvents,
                               ConfigReader configReader, SiddhiQueryContext siddhiQueryContext) {
        this.processingMode = processingMode;
        this.outputExpectsExpiredEvents = outputExpectsExpiredEvents;
        this.siddhiQueryContext = siddhiQueryContext;
        this.configReader = configReader;
        try {
            this.attributeExpressionExecutors = attributeExpressionExecutors;
            this.attributeSize = attributeExpressionExecutors.length;
            init(attributeExpressionExecutors, processingMode, outputExpectsExpiredEvents,
                    configReader, siddhiQueryContext);
        } catch (Throwable t) {
            throw new SiddhiAppCreationException(t);
        }
    }

    public AttributeAggregator cloneAggregator(String key) {
        try {
            AttributeAggregator attributeAggregator = this.getClass().newInstance();
            ExpressionExecutor[] innerExpressionExecutors = new ExpressionExecutor[attributeSize];
            for (int i = 0; i < attributeSize; i++) {
                innerExpressionExecutors[i] = attributeExpressionExecutors[i].cloneExecutor(key);
            }
            attributeAggregator.initAggregator(innerExpressionExecutors, processingMode, outputExpectsExpiredEvents,
                    configReader, siddhiQueryContext);
            return attributeAggregator;
        } catch (Exception e) {
            throw new SiddhiAppRuntimeException("Exception in cloning " + this.getClass().getCanonicalName(), e);
        }
    }

    public synchronized Object process(ComplexEvent event) {
        if (attributeSize > 1) {
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
                    return reset();
            }
        } else if (attributeSize == 1) {
            switch (event.getType()) {
                case CURRENT:
                    return processAdd(attributeExpressionExecutors[0].execute(event));
                case EXPIRED:
                    return processRemove(attributeExpressionExecutors[0].execute(event));
                case RESET:
                    return reset();
            }
        } else {
            switch (event.getType()) {
                case CURRENT:
                    return processAdd(null);
                case EXPIRED:
                    return processRemove(null);
                case RESET:
                    return reset();
            }
        }
        return null;
    }

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param processingMode               query processing mode
     * @param outputExpectsExpiredEvents   is expired events sent as output
     * @param configReader                 this hold the {@link AttributeAggregator} extensions configuration reader.
     * @param siddhiQueryContext           Siddhi query runtime context
     */
    protected abstract void init(ExpressionExecutor[] attributeExpressionExecutors, ProcessingMode processingMode,
                                 boolean outputExpectsExpiredEvents, ConfigReader configReader,
                                 SiddhiQueryContext siddhiQueryContext);

    public abstract Attribute.Type getReturnType();

    public abstract Object processAdd(Object data);

    public abstract Object processAdd(Object[] data);

    public abstract Object processRemove(Object data);

    public abstract Object processRemove(Object[] data);

    public abstract boolean canDestroy();

    public abstract Object reset();

    public abstract Map<String, Object> currentState();

    public abstract void restoreState(Map<String, Object> state);

    public void clean() {
        for (ExpressionExecutor expressionExecutor : attributeExpressionExecutors) {
            expressionExecutor.clean();
        }
    }
}
