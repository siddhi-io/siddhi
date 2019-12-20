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
package io.siddhi.core.event.stream.converter;

import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.Event;
import io.siddhi.core.event.stream.StreamEvent;

import java.io.Serializable;
import java.util.List;

/**
 * The converter class that converts the events into StreamEvent
 */
public class SelectiveStreamEventConverter implements StreamEventConverter, Serializable {

    private static final long serialVersionUID = 5728843379822962369L;
    private List<ConversionMapping> conversionMappings;       //List to hold information needed for conversion

    public SelectiveStreamEventConverter(List<ConversionMapping> conversionMappings) {
        this.conversionMappings = conversionMappings;
    }

    public void convertData(long timestamp, Object[] data, StreamEvent.Type type, StreamEvent newEvent) {
        for (ConversionMapping conversionMapping : conversionMappings) {
            int[] position = conversionMapping.getToPosition();
            int fromPosition = conversionMapping.getFromPosition();
            switch (position[0]) {
                case 0:
                    newEvent.setBeforeWindowData(data[fromPosition], position[1]);
                    break;
                case 1:
                    newEvent.setOnAfterWindowData(data[fromPosition], position[1]);
                    break;
                case 2:
                    newEvent.setOutputData(data[fromPosition], position[1]);
                    break;
                default:
                    //can not happen
            }
        }

        newEvent.setType(type);
        newEvent.setTimestamp(timestamp);
    }


    public void convertEvent(Event event, StreamEvent newEvent) {
        convertData(event.getTimestamp(), event.getData(), event.isExpired() ? StreamEvent.Type.EXPIRED : StreamEvent
                        .Type.CURRENT,
                newEvent);
    }

    public void convertComplexEvent(ComplexEvent complexEvent, StreamEvent newEvent) {
        convertData(complexEvent.getTimestamp(), complexEvent.getOutputData(), complexEvent.getType(),
                newEvent);
    }

    @Override
    public void convertData(long timeStamp, Object[] data, StreamEvent newEvent) {
        convertData(timeStamp, data, StreamEvent.Type.CURRENT, newEvent);
    }

}
