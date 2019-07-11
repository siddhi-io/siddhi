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
package io.siddhi.core.query.input.stream.join;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.MetaComplexEvent;
import io.siddhi.core.event.state.MetaStateEvent;
import io.siddhi.core.query.input.stream.StreamRuntime;
import io.siddhi.core.query.input.stream.single.SingleStreamRuntime;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.core.query.selector.QuerySelector;

import java.util.ArrayList;
import java.util.List;

/**
 * StreamRuntime implementation to represent Join streams.
 */
public class JoinStreamRuntime implements StreamRuntime {

    private List<SingleStreamRuntime> singleStreamRuntimeList = new ArrayList<SingleStreamRuntime>();
    private MetaStateEvent metaStateEvent;
    private ProcessingMode overallProcessingMode = ProcessingMode.BATCH;
    private QuerySelector querySelector;

    public JoinStreamRuntime(SiddhiQueryContext siddhiQueryContext, MetaStateEvent metaStateEvent) {

        this.metaStateEvent = metaStateEvent;
        this.querySelector = null;
    }


    public void addRuntime(SingleStreamRuntime singleStreamRuntime) {
        overallProcessingMode = ProcessingMode.findUpdatedProcessingMode(overallProcessingMode,
                singleStreamRuntime.getProcessingMode());
        singleStreamRuntimeList.add(singleStreamRuntime);
    }

    public List<SingleStreamRuntime> getSingleStreamRuntimes() {
        return singleStreamRuntimeList;
    }

    @Override
    public void setCommonProcessor(Processor commonProcessor) {
        for (SingleStreamRuntime singleStreamRuntime : singleStreamRuntimeList) {
            singleStreamRuntime.setCommonProcessor(commonProcessor);
        }
    }

    @Override
    public MetaComplexEvent getMetaComplexEvent() {
        return metaStateEvent;
    }

    @Override
    public QuerySelector getQuerySelector() {
        return this.querySelector;
    }

    public void setQuerySelector(QuerySelector querySelector) {
        this.querySelector = querySelector;
    }

    public ProcessingMode getProcessingMode() {
        return overallProcessingMode;
    }
}
