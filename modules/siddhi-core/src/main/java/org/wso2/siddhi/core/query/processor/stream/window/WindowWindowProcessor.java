/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.siddhi.core.query.processor.stream.window;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.table.EventTable;
import org.wso2.siddhi.core.util.collection.operator.Finder;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaStateHolder;
import org.wso2.siddhi.core.window.EventWindow;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.List;
import java.util.Map;

/**
 * This is the {@link WindowProcessor} intended to be used with window join queries.
 * This processor keeps a reference of the {@link EventWindow} and directly find
 * the items from the {@link EventWindow}.
 * The process method just passes the events to the next
 * {@link org.wso2.siddhi.core.query.input.stream.join.JoinProcessor} inorder to handle
 * the events there.
 */
public class WindowWindowProcessor extends WindowProcessor implements FindableProcessor {

    /**
     * {@link EventWindow} from which the events have to be found.
     */
    private EventWindow eventWindow;

    public WindowWindowProcessor(EventWindow eventWindow) {
        this.eventWindow = eventWindow;
    }


    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        // nothing to be done
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner) {
        // Pass the event  to the post JoinProcessor
        nextProcessor.process(streamEventChunk);
    }


    @Override
    public StreamEvent find(StateEvent matchingEvent, Finder finder) {
        return eventWindow.find(matchingEvent, finder);
    }

    @Override
    public Finder constructFinder(Expression expression, MatchingMetaStateHolder matchingMetaStateHolder, ExecutionPlanContext executionPlanContext, List<VariableExpressionExecutor> variableExpressionExecutors, Map<String, EventTable> eventTableMap) {
        return eventWindow.constructFinder(expression, matchingMetaStateHolder, executionPlanContext, variableExpressionExecutors, eventTableMap);
    }

    @Override
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        //Do nothing
    }

    @Override
    public Processor cloneProcessor(String key) {
        try {
            WindowWindowProcessor streamProcessor = new WindowWindowProcessor(eventWindow);
            streamProcessor.inputDefinition = inputDefinition;
            ExpressionExecutor[] innerExpressionExecutors = new ExpressionExecutor[attributeExpressionLength];
            ExpressionExecutor[] attributeExpressionExecutors1 = this.attributeExpressionExecutors;
            for (int i = 0; i < attributeExpressionLength; i++) {
                innerExpressionExecutors[i] = attributeExpressionExecutors1[i].cloneExecutor(key);
            }
            streamProcessor.attributeExpressionExecutors = innerExpressionExecutors;
            streamProcessor.attributeExpressionLength = attributeExpressionLength;
            streamProcessor.additionalAttributes = additionalAttributes;
            streamProcessor.complexEventPopulater = complexEventPopulater;
            streamProcessor.init(inputDefinition, attributeExpressionExecutors, executionPlanContext, outputExpectsExpiredEvents);
            streamProcessor.start();
            return streamProcessor;

        } catch (Exception e) {
            throw new ExecutionPlanRuntimeException("Exception in cloning " + this.getClass().getCanonicalName(), e);
        }
    }

    @Override
    public Object[] currentState() {
        //No state
        return null;
    }

    @Override
    public void restoreState(Object[] state) {
        //Nothing to be done
    }
}
