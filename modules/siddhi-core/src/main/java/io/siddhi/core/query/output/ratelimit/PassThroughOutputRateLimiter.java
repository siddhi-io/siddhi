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
package io.siddhi.core.query.output.ratelimit;


import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.util.snapshot.state.StateFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Implementation of {@link OutputRateLimiter} which will pass through events without doing any rate limiting. This
 * is the default rate limiting strategy used by Siddhi.
 */
public class PassThroughOutputRateLimiter extends OutputRateLimiter {
    private static final Logger log = LogManager.getLogger(PassThroughOutputRateLimiter.class);
    private String id;

    public PassThroughOutputRateLimiter(String id) {
        this.id = id;
    }

    @Override
    protected StateFactory init() {
        return null;
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        sendToCallBacks(complexEventChunk);
    }

    @Override
    public void partitionCreated() {
        //Nothing to be done
    }

}
