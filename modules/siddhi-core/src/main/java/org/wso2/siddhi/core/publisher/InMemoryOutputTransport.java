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

package org.wso2.siddhi.core.publisher;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.OutputTransportException;
import org.wso2.siddhi.core.exception.TestConnectionNotSupportedException;

import java.util.Map;

public class InMemoryOutputTransport extends OutputTransport {
    private static final Logger log = Logger.getLogger(InMemoryOutputTransport.class);

    @Override
    public void init(Map<String, String> transportOptions, ExecutionPlanContext executionPlanContext) throws OutputTransportException {
        log.info("InMemoryOutputTransport:init()");
    }

    @Override
    public void testConnect() throws TestConnectionNotSupportedException, ConnectionUnavailableException {
        log.info("InMemoryOutputTransport:testConnect()");
    }

    @Override
    public void connect() throws ConnectionUnavailableException {
        log.info("InMemoryOutputTransport:connect()");
    }

    @Override
    public void publish(ComplexEvent complexEvent, OutputMapper mapper) throws ConnectionUnavailableException {
        log.info("InMemoryOutputTransport:publish() | event : " + complexEvent.toString());
    }

    @Override
    public void disconnect() {
        log.info("InMemoryOutputTransport:disconnect()");
    }

    @Override
    public void destroy() {
        log.info("InMemoryOutputTransport:destroy()");
    }

    @Override
    public boolean isPolled() {
        return false;
    }
}