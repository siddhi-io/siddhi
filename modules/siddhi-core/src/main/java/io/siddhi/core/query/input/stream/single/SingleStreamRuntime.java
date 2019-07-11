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

import io.siddhi.core.event.MetaComplexEvent;
import io.siddhi.core.query.input.ProcessStreamReceiver;
import io.siddhi.core.query.input.stream.StreamRuntime;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.selector.QuerySelector;

import java.util.ArrayList;
import java.util.List;

/**
 * StreamRuntime to represent Single streams. Ex: Streams to filter queries.
 */
public class SingleStreamRuntime implements StreamRuntime {

    private Processor processorChain;
    private ProcessingMode overallProcessingMode;
    private MetaComplexEvent metaComplexEvent;
    private ProcessStreamReceiver processStreamReceiver;

    public SingleStreamRuntime(ProcessStreamReceiver processStreamReceiver, Processor processorChain,
                               ProcessingMode overallProcessingMode,
                               MetaComplexEvent metaComplexEvent) {
        this.processStreamReceiver = processStreamReceiver;
        this.processorChain = processorChain;
        this.overallProcessingMode = overallProcessingMode;
        this.metaComplexEvent = metaComplexEvent;
    }

    public Processor getProcessorChain() {
        return processorChain;
    }

    public void setProcessorChain(Processor processorChain) {
        this.processorChain = processorChain;
    }

    public ProcessStreamReceiver getProcessStreamReceiver() {
        return processStreamReceiver;
    }

    @Override
    public List<SingleStreamRuntime> getSingleStreamRuntimes() {
        List<SingleStreamRuntime> list = new ArrayList<SingleStreamRuntime>(1);
        list.add(this);
        return list;

    }

    @Override
    public void setCommonProcessor(Processor commonProcessor) {
        if (processorChain == null) {
            processStreamReceiver.setNext(commonProcessor);
        } else {
            processStreamReceiver.setNext(processorChain);
            processorChain.setToLast(commonProcessor);
        }
    }

    @Override
    public QuerySelector getQuerySelector() {
        return null;
    }

    public MetaComplexEvent getMetaComplexEvent() {
        return metaComplexEvent;
    }

    public ProcessingMode getProcessingMode() {
        return overallProcessingMode;
    }
}
