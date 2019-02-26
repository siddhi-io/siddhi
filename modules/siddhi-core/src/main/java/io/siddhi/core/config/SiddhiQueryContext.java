/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.core.config;

import io.siddhi.core.util.statistics.LatencyTracker;
import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.api.execution.query.output.stream.OutputStream;

/**
 * Holder object for context information of {@link SiddhiApp}.
 */
public class SiddhiQueryContext {

    private SiddhiAppContext siddhiAppContext = null;
    private String name;
    private OutputStream.OutputEventType outputEventType;
    private LatencyTracker latencyTracker;

    public SiddhiQueryContext(SiddhiAppContext siddhiAppContext, String queryName) {

        this.siddhiAppContext = siddhiAppContext;
        this.name = queryName;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public SiddhiAppContext getSiddhiAppContext() {
        return siddhiAppContext;
    }

    public SiddhiContext getSiddhiContext() {
        return siddhiAppContext.getSiddhiContext();
    }

    public void setSiddhiAppContext(SiddhiAppContext siddhiAppContext) {
        this.siddhiAppContext = siddhiAppContext;
    }

    public void setOutputEventType(OutputStream.OutputEventType outputEventType) {
        this.outputEventType = outputEventType;
    }

    public OutputStream.OutputEventType getOutputEventType() {
        return outputEventType;
    }

    public void setLatencyTracker(LatencyTracker latencyTracker) {
        this.latencyTracker = latencyTracker;
    }

    public LatencyTracker getLatencyTracker() {
        return latencyTracker;
    }
}
