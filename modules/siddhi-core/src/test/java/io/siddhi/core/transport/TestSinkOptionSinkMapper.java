/*
 * Copyright (c)  2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.core.transport;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.output.sink.SinkListener;
import io.siddhi.core.stream.output.sink.SinkMapper;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.transport.Option;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.core.util.transport.TemplateBuilder;
import io.siddhi.query.api.definition.StreamDefinition;

import java.util.Map;

/**
 * Implementation of {@link SinkMapper} representing pass-through scenario where no mapping is done and
 * {@link Event}s are send directly to transports.
 */
@Extension(
        name = "testSinkOption",
        namespace = "sinkMapper",
        description = "Pass-through mapper passed events (Event[]) through without any mapping or modifications.",
        examples = @Example(
                syntax = "@sink(type='inMemory', @map(type='passThrough'))\n" +
                        "define stream BarStream (symbol string, price float, volume long);",
                description = "In the following example BarStream uses passThrough outputmapper which emit " +
                        "Siddhi event directly without any transformation into sink."
        )
)
public class TestSinkOptionSinkMapper extends SinkMapper {

    private Option topicOption;
    private String prefix;
    private boolean mapType;

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[0];
    }

    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder, Map<String, TemplateBuilder>
            payloadTemplateBuilderMap, ConfigReader mapperConfigReader, SiddhiAppContext siddhiAppContext) {
        topicOption = sinkOptionHolder.validateAndGetOption("topic");
        prefix = sinkOptionHolder.validateAndGetStaticValue("prefix");
        mapType = Boolean.valueOf(optionHolder.validateAndGetStaticValue("mapType", "false"));
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{Event[].class, Event.class};
    }

    @Override
    public void mapAndSend(Event[] events, OptionHolder optionHolder,
                           Map<String, TemplateBuilder> payloadTemplateBuilderMap, SinkListener sinkListener) {
        for (Event event : events) {
            String topic = topicOption.getValue(event);
            if (mapType) {
                for (TemplateBuilder templateBuilder : payloadTemplateBuilderMap.values()) {
                    sinkListener.publish(new Event(System.currentTimeMillis(), new Object[]{sinkType, topic,
                            templateBuilder.isObjectMessage() + "-" + templateBuilder.getType().toString()}));
                }
            } else {
                //to change the topic as topic is picked from the original event
                event.getData()[0] = sinkType;
                sinkListener.publish(new Event(System.currentTimeMillis(), new Object[]{sinkType, topic, prefix}));
            }
        }
    }

    @Override
    public void mapAndSend(Event event, OptionHolder optionHolder,
                           Map<String, TemplateBuilder> payloadTemplateBuilderMap, SinkListener sinkListener) {
        String topic = topicOption.getValue(event);
        if (mapType) {
            for (TemplateBuilder templateBuilder : payloadTemplateBuilderMap.values()) {
                sinkListener.publish(new Event(System.currentTimeMillis(), new Object[]{sinkType, topic,
                        templateBuilder.isObjectMessage() + "-" + templateBuilder.getType().toString()}));
            }
        } else {
            //to change the topic as topic is picked from the original event
            event.getData()[0] = sinkType;
            sinkListener.publish(new Event(System.currentTimeMillis(), new Object[]{sinkType, topic, prefix}));
        }
    }
}
