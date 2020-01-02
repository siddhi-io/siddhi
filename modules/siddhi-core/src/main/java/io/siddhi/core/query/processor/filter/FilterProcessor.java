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
package io.siddhi.core.query.processor.filter;

import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.exception.OperationNotSupportedException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.query.api.definition.Attribute;

import java.util.List;

/**
 * Implementation of {@link Processor} which handles Filter expressions in Siddhi.
 */
public class FilterProcessor implements Processor {

    protected Processor next;
    private ExpressionExecutor conditionExecutor;

    public FilterProcessor(ExpressionExecutor conditionExecutor) {
        if (Attribute.Type.BOOL.equals(conditionExecutor.getReturnType())) {
            this.conditionExecutor = conditionExecutor;
        } else {
            throw new OperationNotSupportedException("Return type of " + conditionExecutor.toString() + " should be " +
                    "of type BOOL. " +
                    "Actual type: " + conditionExecutor.getReturnType().toString());
        }
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        complexEventChunk.reset();
        while (complexEventChunk.hasNext()) {
            ComplexEvent complexEvent = complexEventChunk.next();
            Object result = conditionExecutor.execute(complexEvent);
            if (result == null || !(Boolean) result) {
                complexEventChunk.remove();
            }
        }
        if (complexEventChunk.getFirst() != null) {
            this.next.process(complexEventChunk);
        }
    }


    @Override
    public void process(List<ComplexEventChunk> complexEventChunks) {
        ComplexEventChunk complexEventChunk = new ComplexEventChunk();
        for (ComplexEventChunk streamEventChunk : complexEventChunks) {
            complexEventChunk.addAll(streamEventChunk);
        }
        process(complexEventChunk);
    }

    @Override
    public Processor getNextProcessor() {
        return next;
    }

    @Override
    public void setNextProcessor(Processor processor) {
        next = processor;
    }

    @Override
    public void setToLast(Processor processor) {
        if (next == null) {
            this.next = processor;
        } else {
            this.next.setToLast(processor);
        }
    }

}
