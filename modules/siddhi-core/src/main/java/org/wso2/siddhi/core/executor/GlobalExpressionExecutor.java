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


import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.MetaComplexEvent;
import org.wso2.siddhi.core.event.state.MetaStateEvent;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.util.lock.LockWrapper;
import org.wso2.siddhi.core.util.parser.ExpressionParser;
import org.wso2.siddhi.core.util.parser.helper.ParameterWrapper;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * {@link GlobalExpressionExecutor} is both an expression and a findable processor to represent the Siddhi
 * global variables.
 */
public class GlobalExpressionExecutor implements ExpressionExecutor {

    /**
     * LockWrapper to coordinate asynchronous events.
     */
    private final LockWrapper lockWrapper;
    private String id;
    private Expression expression;
    private Attribute.Type returnType;
    private Map<String, ExpressionExecutor> expressionExecutorMap = new ConcurrentHashMap<>();
    private Map<String, QueryInfo> queryInfoMap = new ConcurrentHashMap<>();

    public GlobalExpressionExecutor(String id, Expression expression) {
        this.id = id;
        this.expression = expression;
        this.lockWrapper = new LockWrapper(id);
        this.lockWrapper.setLock(new ReentrantLock());
    }

    @Override
    public Object execute(ComplexEvent event) {
        String streamId;
        if (event instanceof StreamEvent) {
            streamId = ((StreamEvent) event).getStreamId();
        } else {
            streamId = ((StateEvent) event).getStreamEvent(0).getStreamId();
        }
        ExpressionExecutor executor = expressionExecutorMap.get(streamId);
        if (executor != null) {
            return executor.execute(event);
        }
        return null;
    }

    public Expression getExpression() {
        return expression;
    }

    @Override
    public Attribute.Type getReturnType() {
        return this.returnType;
    }

    @Override
    public ExpressionExecutor cloneExecutor(String key) {
        return this;
    }

    public void addExecutor(MetaComplexEvent metaEvent,
                            int currentState, ParameterWrapper parameterWrapper,
                            SiddhiAppContext siddhiAppContext,
                            boolean groupBy, int defaultStreamEventIndex, String queryName) {
        MetaStreamEvent metaStreamEvent;
        if (metaEvent instanceof MetaStreamEvent) {
            metaStreamEvent = (MetaStreamEvent) metaEvent;
        } else {
            metaStreamEvent = ((MetaStateEvent) metaEvent).getMetaStreamEvents()[0];
        }
        QueryInfo queryInfo = new QueryInfo(metaEvent, currentState, parameterWrapper, siddhiAppContext, groupBy,
                defaultStreamEventIndex, queryName);
        ExpressionExecutor executor = queryInfo.createExecutor();
        queryInfoMap.putIfAbsent(metaStreamEvent.getStreamId(), queryInfo);
        if (returnType == null) {
            returnType = executor.getReturnType();
        } else if (returnType != executor.getReturnType()) {
            throw new SiddhiAppValidationException("Expression executor " + id + " is already assigned with " +
                    returnType);
        }
        this.expressionExecutorMap.putIfAbsent(metaStreamEvent.getStreamId(), executor);
    }

    public ExpressionExecutor getExecutor(MetaComplexEvent metaComplexEvent) {
        MetaStreamEvent metaStreamEvent;
        if (metaComplexEvent instanceof MetaStreamEvent) {
            metaStreamEvent = (MetaStreamEvent) metaComplexEvent;
        } else {
            metaStreamEvent = ((MetaStateEvent) metaComplexEvent).getMetaStreamEvents()[0];
        }
        return this.expressionExecutorMap.get(metaStreamEvent.getStreamId());
    }

    private class QueryInfo {
        private MetaComplexEvent metaEvent;
        private int currentState;
        private ParameterWrapper parameterWrapper;
        private SiddhiAppContext siddhiAppContext;
        private boolean groupBy;
        private int defaultStreamEventIndex;
        private String queryName;

        public QueryInfo(MetaComplexEvent metaEvent, int currentState, ParameterWrapper
                parameterWrapper, SiddhiAppContext siddhiAppContext, boolean groupBy, int defaultStreamEventIndex,
                         String queryName) {
            this.metaEvent = metaEvent;
            this.currentState = currentState;
            this.parameterWrapper = parameterWrapper;
            this.siddhiAppContext = siddhiAppContext;
            this.groupBy = groupBy;
            this.defaultStreamEventIndex = defaultStreamEventIndex;
            this.queryName = queryName;
        }

        public ExpressionExecutor createExecutor() {
            return ExpressionParser.parseExpression(GlobalExpressionExecutor.this.expression, metaEvent,
                    currentState, parameterWrapper, siddhiAppContext, groupBy, defaultStreamEventIndex, queryName);
        }
    }
}
