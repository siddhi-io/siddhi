/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.core.util.event.handler;

import com.lmax.disruptor.EventHandler;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.StreamJunction;

import java.util.LinkedList;
import java.util.List;

/**
 * Interface to be implemented to receive events via handlers.
 */
public class StreamHandler implements EventHandler<EventExchangeHolder> {

    private List<StreamJunction.Receiver> receivers;
    private int batchSize;
    private final String streamName;
    private final String siddhiAppName;
    private List<Event> eventBuffer = new LinkedList<>();
    private static final Logger log = Logger.getLogger(StreamHandler.class);
    private final StreamJunction faultStreamJunction;
    private final StreamJunction.FaultAction faultAction;

    public StreamHandler(List<StreamJunction.Receiver> receivers, int batchSize,
                         String streamName, String siddhiAppName, StreamJunction faultStreamJunction,
                         StreamJunction.FaultAction faultAction) {
        this.receivers = receivers;
        this.batchSize = batchSize;
        this.streamName = streamName;
        this.siddhiAppName = siddhiAppName;
        this.faultStreamJunction = faultStreamJunction;
        this.faultAction = faultAction;
    }

    public void onEvent(EventExchangeHolder eventExchangeHolder, long sequence, boolean endOfBatch) {
        boolean isProcessed = eventExchangeHolder.getAndSetIsProcessed(true);
        if (!isProcessed) {
            eventBuffer.add(eventExchangeHolder.getEvent());
            if (eventBuffer.size() == batchSize || endOfBatch) {
                for (StreamJunction.Receiver receiver : receivers) {
                    try {
                        receiver.receive(eventBuffer);
                    } catch (Throwable t) {
                        onError(eventBuffer, t);
                    }
                }
                eventBuffer.clear();
            }
        } else if (endOfBatch) {
            if (eventBuffer.size() != 0) {
                for (StreamJunction.Receiver receiver : receivers) {
                    try {
                        receiver.receive(eventBuffer);
                    } catch (Throwable t) {
                        onError(eventBuffer, t);
                    }
                }
                eventBuffer.clear();
            }
        }

    }

    private void onError(List<Event> eventBuffer, Throwable t) {
        switch (faultAction) {
            case LOG:
                for (Event event : eventBuffer) {
                    log.error("Error in SiddhiApp '" + siddhiAppName +
                            "' after consuming events from Stream " +
                            "'" + streamName + "', " + t.getMessage() + ". Hence, dropping event '"
                            + event.toString() + "'", t);
                }
                break;
            case STREAM:
                for (Event event : eventBuffer) {
                    if (faultStreamJunction != null) {
                        faultStreamJunction.sendEvent(event);
                    } else {
                        log.error("Error in SiddhiApp '" + siddhiAppName +
                                "' after consuming events from Stream " +
                                "'" + streamName + "', " + t.getMessage()
                                + ". Siddhi Fault Stream for '" + streamName + "' is not defined. "
                                + "Hence dropping the event '" + event.toString() + "'", t);
                    }
                }
                break;
            default:
                break;
        }
    }
}
