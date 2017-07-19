/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.siddhi.extension.input.mapper.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.ReadContext;
import net.minidev.json.JSONArray;
import org.apache.log4j.Logger;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.stream.input.source.AttributeMapping;
import org.wso2.siddhi.core.stream.input.source.InputEventHandler;
import org.wso2.siddhi.core.stream.input.source.SourceMapper;
import org.wso2.siddhi.core.util.AttributeConverter;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This mapper converts JSON string input to {@link org.wso2.siddhi.core.event.ComplexEventChunk}.
 * This extension accepts optional json path expressions to
 * select specific attributes from the stream.
 */

@Extension(
        name = "json",
        namespace = "sourceMapper",
        description = "JSON to Event input mapper. Transports which accepts JSON messages can utilize this extension"
                + "to convert the incoming JSON message to Siddhi event. Users can either send a pre-defined JSON "
                + "format where event conversion will happen without any configs or can use json path to map from a "
                + "custom JSON message.",
        parameters = {
                @Parameter(name = "enclosing.element",
                        description =
                                "Used to specify the enclosing element in case of sending multiple events in same "
                                        + "JSON message. WSO2 DAS will treat the child element of given enclosing "
                                        + "element as events"
                                        + " and execute json path expressions on child elements. If enclosing.element "
                                        + "is not provided "
                                        + "multiple event scenario is disregarded and json path will be evaluated "
                                        + "with respect to "
                                        + "root element.",
                        type = {DataType.STRING}),
                @Parameter(name = "fail.on.missing.attribute",
                        description = "This can either have value true or false. By default it will be true. This "
                                + "attribute allows user to handle unknown attributes. By default if an json "
                                + "execution "
                                + "fails or returns null DAS will drop that message. However setting this property"
                                + " to "
                                + "false will prompt DAS to send and event with null value to Siddhi where user "
                                + "can handle"
                                + " it accordingly(ie. Assign a default value)",
                        type = {DataType.BOOL})
        },
        examples = {
                @Example(
                        syntax = "@source(type='inMemory', topic='stock', @map(type='json'))\n"
                                + "define stream FooStream (symbol string, price float, volume long);\n",
                        description = "Above configuration will do a default JSON input mapping. Expected "
                                + "input will look like below."
                                + "{\n"
                                + "    \"event\":{\n"
                                + "        \"symbol\":\"WSO2\",\n"
                                + "        \"price\":55.6,\n"
                                + "        \"volume\":100\n"
                                + "    }\n"
                                + "}\n"),
                @Example(
                        syntax = "@source(type='inMemory', topic='stock', @map(type='json', "
                                + "enclosing.element=\"$.portfolio\", "
                                + "@attributes(symbol = \"company.symbol\", price = \"price\", volume = \"volume\")))",
                        description = "Above configuration will perform a custom JSON mapping. Expected input will "
                                + "look like below."
                                + "{"
                                + " \"portfolio\":{\n"
                                + "     \"stock\":{"
                                + "        \"volume\":100,\n"
                                + "        \"company\":{\n"
                                + "           \"symbol\":\"WSO2\"\n"
                                + "       },\n"
                                + "        \"price\":55.6\n"
                                + "    }\n"
                                + "}")
        }
)

public class JsonSourceMapper extends SourceMapper {

    private static final String DEFAULT_JSON_MAPPING_PREFIX = "$.";
    private static final String DEFAULT_JSON_EVENT_IDENTIFIER = "event";
    private static final String DEFAULT_ENCLOSING_ELEMENT = "$";
    private static final String FAIL_ON_MISSING_ATTRIBUTE_IDENTIFIER = "fail.on.missing.attribute";
    private static final String ENCLOSING_ELEMENT_IDENTIFIER = "enclosing.element";
    private static final Logger log = Logger.getLogger(JsonSourceMapper.class);

    private StreamDefinition streamDefinition;
    private MappingPositionData[] mappingPositions;
    private List<Attribute> streamAttributes;
    private boolean isCustomMappingEnabled = false;
    private boolean failOnMissingAttribute = true;
    private String enclosingElement = null;
    private AttributeConverter attributeConverter = new AttributeConverter();
    private JsonFactory factory;
    private int attributesSize;

    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder,
                     List<AttributeMapping> attributeMappingList, ConfigReader configReader, SiddhiAppContext
                             siddhiAppContext) {
        this.streamDefinition = streamDefinition;
        this.streamAttributes = this.streamDefinition.getAttributeList();
        attributesSize = this.streamDefinition.getAttributeList().size();
        this.mappingPositions = new MappingPositionData[attributesSize];
        failOnMissingAttribute = Boolean.parseBoolean(optionHolder.
                validateAndGetStaticValue(FAIL_ON_MISSING_ATTRIBUTE_IDENTIFIER, "true"));
        factory = new JsonFactory();
        if (attributeMappingList != null && attributeMappingList.size() > 0) {
            isCustomMappingEnabled = true;
            enclosingElement = optionHolder.validateAndGetStaticValue(ENCLOSING_ELEMENT_IDENTIFIER,
                    DEFAULT_ENCLOSING_ELEMENT);
            for (int i = 0; i < attributeMappingList.size(); i++) {
                AttributeMapping attributeMapping = attributeMappingList.get(i);
                String attributeName = attributeMapping.getName();
                int position;
                if (attributeName != null) {
                    position = this.streamDefinition.getAttributePosition(attributeName);
                } else {
                    position = i;
                }
                this.mappingPositions[i] = new MappingPositionData(position, attributeMapping.getMapping());
            }
        } else {
            for (int i = 0; i < attributesSize; i++) {
                this.mappingPositions[i] = new MappingPositionData(i, DEFAULT_JSON_MAPPING_PREFIX + this
                        .streamDefinition.getAttributeList().get(i).getName());
            }
        }
    }

    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class};
    }

    @Override
    protected void mapAndProcess(Object eventObject, InputEventHandler inputEventHandler) throws InterruptedException {
        Object convertedEvent;
        convertedEvent = convertToEvent(eventObject);
        if (convertedEvent != null) {
            if (convertedEvent instanceof Event[]) {
                inputEventHandler.sendEvents((Event[]) convertedEvent);
            } else {
                inputEventHandler.sendEvent((Event) convertedEvent);
            }
        }
    }

    /**
     * Convert the given JSON string to {@link Event}.
     *
     * @param eventObject JSON string
     * @return the constructed Event object
     */
    private Object convertToEvent(Object eventObject) {
        if (!(eventObject instanceof String)) {
            log.error("Invalid JSON object received. Expected String, but found " +
                    eventObject.getClass()
                            .getCanonicalName());
            return null;
        }

        if (!isJsonValid(eventObject.toString())) {
            log.error("Invalid Json String :" + eventObject.toString());
            return null;
        }

        Object jsonObj;
        ReadContext readContext = JsonPath.parse(eventObject.toString());
        if (isCustomMappingEnabled) {
            jsonObj = readContext.read(enclosingElement);
            if (jsonObj == null) {
                log.error("Enclosing element " + enclosingElement + " cannot be found in the json string " +
                        eventObject.toString() + ".");
                return null;
            }
            if (jsonObj instanceof JSONArray) {
                JSONArray jsonArray = (JSONArray) jsonObj;
                List<Event> eventList = new ArrayList<Event>();
                for (Object eventObj : jsonArray) {
                    Event event = processCustomEvent(JsonPath.parse(eventObj));
                    if (event != null) {
                        eventList.add(event);
                    }
                }
                Event[] eventArray = eventList.toArray(new Event[0]);
                return eventArray;
            } else {
                try {
                    Event event = processCustomEvent(JsonPath.parse(jsonObj));
                    return event;
                } catch (SiddhiAppRuntimeException e) {
                    log.error(e.getMessage());
                    return null;
                }
            }
        } else {
            jsonObj = readContext.read(DEFAULT_ENCLOSING_ELEMENT);
            if (jsonObj instanceof JSONArray) {
                return convertToEventArrayForDefaultMapping(eventObject);
            } else {
                try {
                    return convertToSingleEventForDefaultMapping(eventObject);
                } catch (IOException e) {
                    log.error("Json string " + eventObject + " cannot be parsed to json object.");
                    return null;
                }
            }
        }
    }

    private Event convertToSingleEventForDefaultMapping(Object eventObject) throws IOException {
        Event event = new Event(attributesSize);
        Object[] data = event.getData();
        JsonParser parser;
        int numberOfProvidedAttributes = 0;
        try {
            parser = factory.createParser(eventObject.toString());
        } catch (IOException e) {
            throw new SiddhiAppRuntimeException("Initializing a parser failed for the event string."
                    + eventObject.toString());
        }
        int position;
        while (!parser.isClosed()) {
            JsonToken jsonToken = parser.nextToken();
            if (JsonToken.START_OBJECT.equals(jsonToken)) {
                parser.nextToken();
                if (DEFAULT_JSON_EVENT_IDENTIFIER.equalsIgnoreCase(parser.getText())) {
                    parser.nextToken();
                } else {
                    log.error("Default json message " + eventObject
                            + " contains an invalid event identifier. Required \"event\", " +
                            "but found \"" + parser.getText() + "\". Hence dropping the message.");
                    return null;
                }
            } else if (JsonToken.FIELD_NAME.equals(jsonToken)) {
                String key = parser.getCurrentName();
                numberOfProvidedAttributes++;
                position = findDefaultMappingPosition(key);
                if (position == -1) {
                    log.error("Stream \"" + streamDefinition.getId() +
                            "\" does not have an attribute named \"" + key +
                            "\", but the received event " + eventObject.toString() +
                            " does. Hence dropping the message.");
                    return null;
                }
                jsonToken = parser.nextToken();
                Attribute.Type type = streamAttributes.get(position).getType();

                if (JsonToken.VALUE_NULL.equals(jsonToken)) {
                    data[position] = null;
                } else {
                    switch (type) {
                        case BOOL:
                            if (JsonToken.VALUE_TRUE.equals(jsonToken) || JsonToken.VALUE_FALSE.equals(jsonToken)) {
                                data[position] = parser.getValueAsBoolean();
                            } else {
                                log.error("Json message " + eventObject.toString() +
                                        " contains incompatible attribute types and values. Value " +
                                        parser.getText() + " is not compatible with type BOOL. " +
                                        "Hence dropping the message.");
                                return null;
                            }
                            break;
                        case INT:
                            if (JsonToken.VALUE_NUMBER_INT.equals(jsonToken)) {
                                data[position] = parser.getValueAsInt();
                            } else {
                                log.error("Json message " + eventObject.toString() +
                                        " contains incompatible attribute types and values. Value " +
                                        parser.getText() + " is not compatible with type INT. " +
                                        "Hence dropping the message.");
                                return null;
                            }
                            break;
                        case DOUBLE:
                            if (JsonToken.VALUE_NUMBER_FLOAT.equals(jsonToken)) {
                                data[position] = parser.getValueAsDouble();
                            } else {
                                log.error("Json message " + eventObject.toString() +
                                        " contains incompatible attribute types and values. Value " +
                                        parser.getText() + " is not compatible with type DOUBLE. " +
                                        "Hence dropping the message.");
                                return null;
                            }
                            break;
                        case STRING:
                            if (JsonToken.VALUE_STRING.equals(jsonToken)) {
                                data[position] = parser.getValueAsString();
                            } else {
                                log.error("Json message " + eventObject.toString() +
                                        " contains incompatible attribute types and values. Value " +
                                        parser.getText() + " is not compatible with type STRING. " +
                                        "Hence dropping the message.");
                                return null;
                            }
                            break;
                        case FLOAT:
                            if (JsonToken.VALUE_NUMBER_FLOAT.equals(jsonToken) ||
                                    JsonToken.VALUE_NUMBER_INT.equals(jsonToken)) {
                                data[position] = attributeConverter.getPropertyValue(parser.getValueAsString(),
                                        Attribute.Type.FLOAT);
                            } else {
                                log.error("Json message " + eventObject.toString() +
                                        " contains incompatible attribute types and values. Value " +
                                        parser.getText() + " is not compatible with type FLOAT. " +
                                        "Hence dropping the message.");
                                return null;
                            }
                            break;
                        case LONG:
                            if (JsonToken.VALUE_NUMBER_INT.equals(jsonToken)) {
                                data[position] = parser.getValueAsLong();
                            } else {
                                log.error("Json message " + eventObject.toString() +
                                        " contains incompatible attribute types and values. Value " +
                                        parser.getText() + " is not compatible with type LONG. " +
                                        "Hence dropping the message.");
                                return null;
                            }
                            break;
                        default:
                            return null;
                    }
                }
            }
        }

        if (failOnMissingAttribute && (numberOfProvidedAttributes != attributesSize)) {
            log.error("Json message " + eventObject.toString() +
                    " contains missing attributes. Hence dropping the message.");
            return null;
        }
        return event;
    }

    private Event[] convertToEventArrayForDefaultMapping(Object eventObject) {
        Gson gson = new Gson();
        JsonObject[] eventObjects = gson.fromJson(eventObject.toString(), JsonObject[].class);
        Event[] events = new Event[eventObjects.length];
        int index = 0;
        JsonObject eventObj = null;
        for (JsonObject jsonEvent : eventObjects) {
            if (jsonEvent.has(DEFAULT_JSON_EVENT_IDENTIFIER)) {
                eventObj = jsonEvent.get(DEFAULT_JSON_EVENT_IDENTIFIER).getAsJsonObject();
                if (failOnMissingAttribute && eventObj.size() < streamAttributes.size()) {
                    log.error("Json message " + eventObj.toString() + " contains missing attributes. " +
                            "Hence dropping the message.");
                    continue;
                }
            } else {
                log.error("Default json message " + eventObj.toString()
                        + " in the array does not have the valid event identifier \"event\". " +
                        "Hence dropping the message.");
                continue;
            }
            Event event = new Event(streamAttributes.size());
            Object[] data = event.getData();


            int position = 0;
            for (Attribute attribute : streamAttributes) {
                String attributeName = attribute.getName();
                Attribute.Type type = attribute.getType();
                Object attributeValue = eventObj.get(attributeName);
                if (attributeValue == null) {
                    data[position++] = null;
                } else {
                    data[position++] = attributeConverter.getPropertyValue(
                            eventObj.get(attributeName).toString(), type);
                }
            }
            events[index++] = event;
        }
        return Arrays.copyOfRange(events, 0, index);
    }

    private Event processCustomEvent(ReadContext readContext) {
        Configuration conf = Configuration.defaultConfiguration();
        Event event = new Event(attributesSize);
        Object[] data = event.getData();
        Object childObject = readContext.read(DEFAULT_ENCLOSING_ELEMENT);
        readContext = JsonPath.using(conf).parse(childObject);
        for (MappingPositionData mappingPositionData : this.mappingPositions) {
            int position = mappingPositionData.getPosition();
            Object mappedValue;
            try {
                mappedValue = readContext.read(mappingPositionData.getMapping());
                if (mappedValue == null) {
                    data[position] = null;
                } else {
                    data[position] = attributeConverter.getPropertyValue(mappedValue.toString(),
                            streamAttributes.get(position).getType());
                }
            } catch (PathNotFoundException e) {
                if (failOnMissingAttribute) {
                    log.error("Json message " + childObject.toString() +
                            " contains missing attributes. Hence dropping the message.");
                    return null;
                }
                data[position] = null;
            }
        }
        return event;
    }

    private int findDefaultMappingPosition(String key) {
        for (int i = 0; i < streamAttributes.size(); i++) {
            String attributeName = streamAttributes.get(i).getName();
            if (attributeName.equals(key)) {
                return i;
            }
        }
        return -1;
    }

    private boolean isJsonValid(String jsonInString) {
        Gson gson = new Gson();
        try {
            gson.fromJson(jsonInString, Object.class);
            return true;
        } catch (com.google.gson.JsonSyntaxException ex) {
            return false;
        }
    }

    /**
     * A POJO class which holds the attribute position in output stream and the user defined mapping.
     */
    private static class MappingPositionData {
        /**
         * Attribute position in the output stream.
         */
        private int position;

        /**
         * The JSON mapping as defined by the user.
         */
        private String mapping;

        public MappingPositionData(int position, String mapping) {
            this.position = position;
            this.mapping = mapping;
        }

        public int getPosition() {
            return position;
        }

        public void setPosition(int position) {
            this.position = position;
        }

        public String getMapping() {
            return mapping;
        }

        public void setMapping(String mapping) {
            this.mapping = mapping;
        }
    }
}
