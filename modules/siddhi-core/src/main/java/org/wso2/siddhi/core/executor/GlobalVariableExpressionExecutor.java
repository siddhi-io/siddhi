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

package org.wso2.siddhi.core.executor;


import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.query.processor.stream.window.FindableProcessor;
import org.wso2.siddhi.core.table.Table;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaInfoHolder;
import org.wso2.siddhi.core.util.lock.LockWrapper;
import org.wso2.siddhi.core.util.snapshot.Snapshotable;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

/**
 * {@link GlobalVariableExpressionExecutor} is both an expression and a findable processor to represent the Siddhi
 * global variables.
 */
public class GlobalVariableExpressionExecutor extends RuntimeVariableExpressionExecutor implements FindableProcessor,
        Snapshotable {

    /**
     * LockWrapper to coordinate asynchronous events.
     */
    private final LockWrapper lockWrapper;
    private String id;
    private Attribute.Type type;
    private Object value;
    private List<VariableUpdateListener> variableUpdateListeners = new ArrayList<>();

    public GlobalVariableExpressionExecutor(String id, Attribute.Type type, Object value) {
        this.id = id;
        this.type = type;
        this.lockWrapper = new LockWrapper(id);
        this.lockWrapper.setLock(new ReentrantLock());

        if (value == null) {
            // Assign default value
            if (type == Attribute.Type.BOOL) {
                this.value = false;
            } else if (type == Attribute.Type.INT) {
                this.value = 0;
            } else if (type == Attribute.Type.FLOAT) {
                this.value = 0.0f;
            } else if (type == Attribute.Type.LONG) {
                this.value = 0L;
            } else if (type == Attribute.Type.DOUBLE) {
                this.value = 0.0d;
            } else if (type == Attribute.Type.STRING) {
                this.value = "";
            }
        } else {
            this.value = value;
        }
    }

    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> state) {

    }

    @Override
    public String getElementId() {
        return this.id;
    }

    @Override
    public StreamEvent find(StateEvent matchingEvent, CompiledCondition compiledCondition) {
        return null;
    }

    @Override
    public CompiledCondition compileCondition(Expression expression, MatchingMetaInfoHolder matchingMetaInfoHolder,
                                              ExecutionPlanContext executionPlanContext,
                                              List<VariableExpressionExecutor> variableExpressionExecutors,
                                              Map<String, Table> tableMap, Map<String,
            GlobalVariableExpressionExecutor> variableMap, String queryName) {
        return null;
    }

    @Override
    public Object execute(ComplexEvent event) {
        return this.getValue();
    }

    public Attribute.Type getReturnType() {
        return type;
    }

    @Override
    public ExpressionExecutor cloneExecutor(String key) {
        return this;
    }

    public void update(ComplexEventChunk complexEventChunk) {
        if (complexEventChunk.getFirst() != null) {
            if (complexEventChunk.getFirst().getOutputData().length > 0) {

                Object newValue = complexEventChunk.getFirst().getOutputData()[0];

                if (newValue == null) {
                    throw new RuntimeException("Global variable value cannot be null");
                } else if (type == Attribute.Type.OBJECT) {
                    throw new RuntimeException("GlobalVariable does not support object values");
                } else if (!type.getClazz().equals(newValue.getClass())) {
                    throw new ClassCastException("Global variable " + this.id + " expects " + type + " but received "
                            + newValue.getClass().getSimpleName());
                }

                // Update only if the new value is not null and there is a change
                if (!Objects.equals(newValue, this.value)) {
                    try {
                        lockWrapper.lock();

                        Object oldValue = this.value;
                        this.value = newValue;

                        // Update all the listeners about the change
                        for (VariableUpdateListener listener : this.variableUpdateListeners) {
                            listener.onValueUpdate(oldValue, newValue);
                        }
                    } finally {
                        lockWrapper.unlock();
                    }
                }
            }
        }
    }

    @Override
    public Object getValue() {
        try {
            lockWrapper.lock();
            return value;
        } finally {
            lockWrapper.unlock();
        }
    }

    public void addVariableUpdateListener(VariableUpdateListener listener) {
        if (listener != null) {
            try {
                lockWrapper.lock();
                this.variableUpdateListeners.add(listener);
            } finally {
                lockWrapper.unlock();
            }
        }
    }

    /**
     * Listener to listen the value updates of {@link GlobalVariableExpressionExecutor}.
     */
    public interface VariableUpdateListener {
        void onValueUpdate(Object oldValue, Object newValue);
    }
}
