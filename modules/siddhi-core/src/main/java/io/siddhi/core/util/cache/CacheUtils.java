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
package io.siddhi.core.util.cache;

import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.event.state.StateEvent;
import io.siddhi.core.event.stream.StreamEvent;

/**
 * class containing utils related to store table cache
 */
public class CacheUtils {
    public static int findEventChunkSize(StreamEvent streamEvent) {
        int chunkSize = 1;
        if (streamEvent == null) {
            return 0;
        }
        StreamEvent streamEventCopy = streamEvent;
        while (streamEventCopy.hasNext()) {
            chunkSize = chunkSize + 1;
            streamEventCopy = streamEventCopy.getNext();
        }
        return chunkSize;
    }

    public static void addRequiredFieldsToDataForCache(Object addingEventChunkForCache, Object event,
                                                       SiddhiAppContext siddhiAppContext, String cachePolicy,
                                                       boolean cacheExpiryEnabled) {
        if (event instanceof StreamEvent) {
            StreamEvent eventForCache = checkPoliocyAndAddFields(event, siddhiAppContext, cachePolicy,
                    cacheExpiryEnabled);
            ((ComplexEventChunk<StreamEvent>) addingEventChunkForCache).add(eventForCache);
        } else if (event instanceof StateEvent) {
            StreamEvent eventForCache = checkPoliocyAndAddFields(((StateEvent) event).getStreamEvent(0),
                    siddhiAppContext, cachePolicy, cacheExpiryEnabled);
            StateEvent stateEvent = new StateEvent(1, eventForCache.getOutputData().length);
            stateEvent.addEvent(0, eventForCache);
            ((ComplexEventChunk<StateEvent>) addingEventChunkForCache).add(stateEvent);
        }
    }

    private static StreamEvent checkPoliocyAndAddFields(Object event,
                                                 SiddhiAppContext siddhiAppContext, String cachePolicy,
                                                 boolean cacheExpiryEnabled) {
        Object[] outputDataForCache = null;
        Object[] outputData = ((StreamEvent) event).getOutputData();
        if (cachePolicy.equals("FIFO")) {
            outputDataForCache = new Object[outputData.length + 1];
            outputDataForCache[outputDataForCache.length - 1] =
                    siddhiAppContext.getTimestampGenerator().currentTime();
        } else if (cacheExpiryEnabled) {
            if (cachePolicy.equals("LRU")) {
                outputDataForCache = new Object[outputData.length + 2];
                outputDataForCache[outputDataForCache.length - 2] =
                        outputDataForCache[outputDataForCache.length - 1] =
                                siddhiAppContext.getTimestampGenerator().currentTime();
            } else if (cachePolicy.equals("LFU")) {
                outputDataForCache = new Object[outputData.length + 2];
                outputDataForCache[outputDataForCache.length - 2] =
                        siddhiAppContext.getTimestampGenerator().currentTime();
                outputDataForCache[outputDataForCache.length - 1] = 1;
            }
        } else {
            if (cachePolicy.equals("LRU")) {
                outputDataForCache = new Object[outputData.length + 1];
                outputDataForCache[outputDataForCache.length - 1] =
                        siddhiAppContext.getTimestampGenerator().currentTime();
            } else if (cachePolicy.equals("LFU")) {
                outputDataForCache = new Object[outputData.length + 1];
                outputDataForCache[outputDataForCache.length - 1] = 1;
            }
        }

        assert outputDataForCache != null;
        System.arraycopy(outputData, 0 , outputDataForCache, 0, outputData.length);
        StreamEvent eventForCache = new StreamEvent(0, 0, outputDataForCache.length);
        eventForCache.setOutputData(outputDataForCache);

        return eventForCache;
    }
}
