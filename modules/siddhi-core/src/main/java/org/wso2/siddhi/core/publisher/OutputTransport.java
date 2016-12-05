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

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.exception.OutputTransportException;
import org.wso2.siddhi.core.exception.TestConnectionNotSupportedException;
import org.wso2.siddhi.core.util.extension.holder.EternalReferencedHolder;
import org.wso2.siddhi.query.api.execution.io.Transport;

import java.util.Map;

/**
 * This is a OutputTransport type. these let users to publish events according to
 * some type. this type can either be local, jms or ws (or any custom extension)
 */
public abstract class OutputTransport implements EternalReferencedHolder {

    /**
     * The init of the transport, this will be called only once before connect() and testConnect()
     *
     * @throws OutputTransportException if there are any configuration errors
     */
    public abstract void init(Transport transportOptions, ExecutionPlanContext executionPlanContext)
            throws OutputTransportException;

    /**
     * Used to test the connection
     *
     * @throws TestConnectionNotSupportedException if test connection is not supported by the transport
     * @throws ConnectionUnavailableException      if it cannot connect to the transport backend
     */
    public abstract void testConnect() throws TestConnectionNotSupportedException, ConnectionUnavailableException;

    /**
     * Will be called to connect to the backend before events are published
     *
     * @throws ConnectionUnavailableException if it cannot connect to the backend
     */
    public abstract void connect() throws ConnectionUnavailableException;

    /**
     * To publish the events
     *
     * @param event          event to be published, which is ideally mapped using the mapper
     * @param dynamicOptions dynamic options for the transport (which are configurable dynamically. i.e email subject)
     * @throws ConnectionUnavailableException if it cannot connect to the backend
     */
    public abstract void publish(Object event, Map<String, String> dynamicOptions) throws ConnectionUnavailableException;


    /**
     * Will be called after all publishing is done, or when ConnectionUnavailableException is thrown
     */
    public abstract void disconnect();

    /**
     * Will be called at the end to clean all the resources consumed
     */
    public abstract void destroy();

    /**
     * Whether events get accumulated at the adopter and clients connect to it to collect events
     *
     * @return is polled
     */
    public abstract boolean isPolled();

    @Override
    public void start() {
        System.out.println("Start");
        try {
            connect();
        } catch (ConnectionUnavailableException e) {
            // executionPlanContext.getScheduledExecutorService().schedule()
            e.printStackTrace();//todo implement exponential back off retry
        }
    }

    @Override
    public void stop() {
        System.out.println("Stop");
        disconnect();
        destroy();
    }
}
