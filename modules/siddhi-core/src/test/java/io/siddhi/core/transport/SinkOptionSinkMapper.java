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
        name = "sinkOption",
        namespace = "sinkMapper",
        description = "Pass-through mapper passed events (Event[]) through without any mapping or modifications.",
        examples = @Example(
                syntax = "@sink(type='inMemory', @map(type='passThrough'))\n" +
                        "define stream BarStream (symbol string, price float, volume long);",
                description = "In the following example BarStream uses passThrough outputmapper which emit " +
                        "Siddhi event directly without any transformation into sink."
        )
)
public class SinkOptionSinkMapper extends SinkMapper {

    private Option topicOption;
    private String prefix;

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[0];
    }

    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder, Map<String, TemplateBuilder>
            payloadTemplateBuilderMap, ConfigReader mapperConfigReader, SiddhiAppContext siddhiAppContext) {
        topicOption = sinkOptionHolder.validateAndGetOption("topic");
        prefix = sinkOptionHolder.validateAndGetStaticValue("prefix");
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
            event.getData()[0] = sinkType;
            event.getData()[1] = topic;
            event.getData()[2] = prefix;
        }
        sinkListener.publish(events);
    }

    @Override
    public void mapAndSend(Event event, OptionHolder optionHolder,
                           Map<String, TemplateBuilder> payloadTemplateBuilderMap, SinkListener sinkListener) {
        String topic = topicOption.getValue(event);
        event.getData()[0] = sinkType;
        event.getData()[1] = topic;
        event.getData()[2] = prefix;
        sinkListener.publish(event);
    }
}
