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

package io.siddhi.core.transport;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.MappingFailedException;
import io.siddhi.core.stream.input.source.AttributeMapping;
import io.siddhi.core.stream.input.source.InputEventHandler;
import io.siddhi.core.stream.input.source.PassThroughSourceMapper;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.error.handler.model.ErroneousEvent;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.query.api.definition.StreamDefinition;

import java.util.Collections;
import java.util.List;

/**
 * Test String Mapper implementation to is used for testing purposes.
 */
@Extension(
        name = "testTrp",
        namespace = "sourceMapper",
        description = "testString mapper passed string events through without any mapping or modifications.",
        examples = @Example(
                syntax = "@source(type='tcp', @map(type='testString'),\n" +
                        "define stream BarStream (symbol string, price float, volume long);",
                description = "In this example BarStream uses passThrough string inputmapper which passes the " +
                        "received string directly without any transformation into source."
        )
)
public class TestTrpSourceMapper extends PassThroughSourceMapper {

    private StreamDefinition streamDefinition;
    private OptionHolder optionHolder;
    private List<AttributeMapping> attributeMappingList;
    private ConfigReader configReader;
    private SiddhiAppContext siddhiAppContext;

    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder,
                     List<AttributeMapping> attributeMappingList, ConfigReader configReader,
                     SiddhiAppContext siddhiAppContext) {
        this.streamDefinition = streamDefinition;
        this.optionHolder = optionHolder;
        this.attributeMappingList = attributeMappingList;
        this.configReader = configReader;
        this.siddhiAppContext = siddhiAppContext;
    }

    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{Event.class};
    }

    @Override
    protected void mapAndProcess(Object eventObject, InputEventHandler inputEventHandler)
            throws MappingFailedException, InterruptedException {
        if (eventObject != null) {
            if (eventObject instanceof Event) {
                Event output = new Event(streamDefinition.getAttributeList().size());
                for (AttributeMapping attributeMapping : attributeMappingList) {
                    output.getData()[attributeMapping.getPosition()] = ((Event) eventObject).getData(
                            Integer.parseInt(attributeMapping.getMapping()));
                }
                inputEventHandler.sendEvent(output);
            } else {
                throw new MappingFailedException(Collections.singletonList(new ErroneousEvent(eventObject,
                        "Event object must be either Event[], Event or Object[] " +
                                "but found " + eventObject.getClass().getCanonicalName())));
            }
        }
    }
}
