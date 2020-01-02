/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.core.query;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.event.MetaComplexEvent;
import io.siddhi.core.query.input.MultiProcessStreamReceiver;
import io.siddhi.core.query.input.stream.StreamRuntime;
import io.siddhi.core.query.input.stream.single.SingleStreamRuntime;
import io.siddhi.core.query.input.stream.state.StateStreamRuntime;
import io.siddhi.core.query.output.callback.OutputCallback;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.query.output.ratelimit.OutputRateLimiter;
import io.siddhi.core.query.selector.QuerySelector;
import io.siddhi.core.util.extension.holder.ExternalReferencedHolder;
import io.siddhi.core.util.statistics.MemoryCalculable;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.stream.JoinInputStream;
import io.siddhi.query.api.execution.query.input.stream.SingleInputStream;
import io.siddhi.query.api.execution.query.input.stream.StateInputStream;

import java.util.List;

/**
 * Query Runtime represent holder object for a single Siddhi query and holds all runtime objects related to that query.
 */
public class QueryRuntimeImpl implements QueryRuntime, MemoryCalculable, ExternalReferencedHolder {

    private StreamRuntime streamRuntime;
    private OutputRateLimiter outputRateLimiter;
    private Query query;
    private OutputCallback outputCallback;
    private SiddhiQueryContext siddhiQueryContext;
    private StreamDefinition outputStreamDefinition;
    private boolean toLocalStream;
    private QuerySelector selector;
    private MetaComplexEvent metaComplexEvent;

    public QueryRuntimeImpl(Query query, StreamRuntime streamRuntime, QuerySelector selector,
                            OutputRateLimiter outputRateLimiter, OutputCallback outputCallback,
                            MetaComplexEvent metaComplexEvent,
                            SiddhiQueryContext siddhiQueryContext) {
        this.query = query;
        this.streamRuntime = streamRuntime;
        this.selector = selector;
        this.outputCallback = outputCallback;
        this.siddhiQueryContext = siddhiQueryContext;
        outputRateLimiter.setOutputCallback(outputCallback);
        setOutputRateLimiter(outputRateLimiter);
        setMetaComplexEvent(metaComplexEvent);
        init();
    }

    public String getQueryId() {
        return siddhiQueryContext.getName();
    }

    public void addCallback(QueryCallback callback) {
        outputRateLimiter.addQueryCallback(callback);
    }

    public void removeCallback(QueryCallback callback) {
        outputRateLimiter.removeQueryCallback(callback);
    }

    public OutputRateLimiter getOutputRateManager() {
        return outputRateLimiter;
    }

    public StreamDefinition getOutputStreamDefinition() {
        return outputStreamDefinition;
    }

    public List<String> getInputStreamId() {
        return query.getInputStream().getAllStreamIds();
    }

    public boolean isToLocalStream() {
        return toLocalStream;
    }

    public void setToLocalStream(boolean toLocalStream) {
        this.toLocalStream = toLocalStream;
    }

    public boolean isStateful() {
        return siddhiQueryContext.isStateful();
    }

    public boolean isFromLocalStream() {
        if (query.getInputStream() instanceof SingleInputStream) {
            return ((SingleInputStream) query.getInputStream()).isInnerStream();
        } else if (query.getInputStream() instanceof JoinInputStream) {
            return ((SingleInputStream) ((JoinInputStream) query.getInputStream()).getLeftInputStream())
                    .isInnerStream() || ((SingleInputStream) ((JoinInputStream) query.getInputStream())
                    .getRightInputStream()).isInnerStream();
        } else if (query.getInputStream() instanceof StateInputStream) {
            for (String streamId : query.getInputStream().getAllStreamIds()) {
                if (streamId.startsWith("#")) {
                    return true;
                }
            }
        }
        return false;
    }

    private void setOutputRateLimiter(OutputRateLimiter outputRateLimiter) {
        this.outputRateLimiter = outputRateLimiter;
        selector.setNextProcessor(outputRateLimiter);
    }

    public SiddhiQueryContext getSiddhiQueryContext() {
        return siddhiQueryContext;
    }

    public StreamRuntime getStreamRuntime() {
        return streamRuntime;
    }

    public MetaComplexEvent getMetaComplexEvent() {
        return metaComplexEvent;
    }

    private void setMetaComplexEvent(MetaComplexEvent metaComplexEvent) {
        outputStreamDefinition = metaComplexEvent.getOutputStreamDefinition();
        this.metaComplexEvent = metaComplexEvent;
    }

    public Query getQuery() {
        return query;
    }

    public OutputCallback getOutputCallback() {
        return outputCallback;
    }

    public void init() {
        streamRuntime.setCommonProcessor(selector);
        for (SingleStreamRuntime singleStreamRuntime : streamRuntime.getSingleStreamRuntimes()) {
            if (singleStreamRuntime.getProcessStreamReceiver() instanceof MultiProcessStreamReceiver) {
                ((MultiProcessStreamReceiver) singleStreamRuntime.getProcessStreamReceiver())
                        .setOutputRateLimiter(outputRateLimiter);
            }
        }
    }

    public QuerySelector getSelector() {
        return selector;
    }

    public void initPartition() {
        if (streamRuntime instanceof StateStreamRuntime) {
            ((StateStreamRuntime) streamRuntime).initPartition();
        }
        outputRateLimiter.partitionCreated();
    }

    public void start() {
        initPartition();
    }

    @Override
    public void stop() {
    }
}
