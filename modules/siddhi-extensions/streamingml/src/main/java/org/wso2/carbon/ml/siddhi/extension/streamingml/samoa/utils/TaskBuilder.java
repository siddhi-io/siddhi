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
package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils;

import org.apache.samoa.topology.Topology;
import org.apache.samoa.topology.impl.SimpleEngine;

import java.util.Queue;

public abstract class TaskBuilder {
    // It seems that the 3 extra options are not used directly, But It used when convert sting to Task object.
    protected static final String SUPPRESS_STATUS_OUT_MSG =
            "Suppress the task status output. Normally it is sent to stderr.";
    protected static final String SUPPRESS_RESULT_OUT_MSG =
            "Suppress the task result output. Normally it is sent to stdout.";
    protected static final String STATUS_UPDATE_FREQ_MSG =
            "Wait time in milliseconds between status updates.";

    public Queue<double[]> cepEvents;
    protected int maxInstances;
    protected int numberOfAttributes;
    protected Topology topology;

    public abstract void initTask();

    public void submit() {
        SimpleEngine.submitTopology(topology);
    }
}



