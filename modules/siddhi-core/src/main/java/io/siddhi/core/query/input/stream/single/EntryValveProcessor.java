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

package io.siddhi.core.query.input.stream.single;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.util.Schedulable;
import io.siddhi.core.util.ThreadBarrier;

import java.util.List;

/**
 * Entry Valve Siddhi processor chain.
 */
public class EntryValveProcessor implements Processor, Schedulable {

    private Processor next;
    private ThreadBarrier threadBarrier;

    public EntryValveProcessor(SiddhiAppContext siddhiAppContext) {
        threadBarrier = siddhiAppContext.getThreadBarrier();
    }


    /**
     * Process the handed StreamEvent
     *
     * @param complexEventChunk event chunk to be processed
     */
    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        threadBarrier.enter();
        try {
            next.process(complexEventChunk);
        } finally {
            threadBarrier.exit();
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

    /**
     * Get next processor element in the processor chain. Processed event should be sent to next processor
     *
     * @return Next Processor
     */

    @Override
    public Processor getNextProcessor() {
        return next;
    }

    /**
     * Set next processor element in processor chain
     *
     * @param processor Processor to be set as next element of processor chain
     */
    @Override
    public void setNextProcessor(Processor processor) {
        next = processor;
    }

    /**
     * Set as the last element of the processor chain
     *
     * @param processor Last processor in the chain
     */
    @Override
    public void setToLast(Processor processor) {
        if (next == null) {
            this.next = processor;
        } else {
            this.next.setToLast(processor);
        }
    }

}
