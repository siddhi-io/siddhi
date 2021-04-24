/*
 * Copyright (c)  2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.core.util.transport;

import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.NoSuchAttributeException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.output.sink.SinkMapper;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Template builder used by {@link SinkMapper} to generate custom payload.
 */
public class TemplateBuilder {

    private static final Pattern DYNAMIC_PATTERN = Pattern.compile("(\\{\\{[^{}]*\\}\\})|[{}]");
    private static final String SPLIT_PATTERN = "(\\{.\\{|\\}.\\})";
    private int[] positionArray;
    private String[] splitTemplateArray;
    private boolean isObjectMessage = false;
    private int objectIndex = -1;
    private Attribute.Type type = Attribute.Type.STRING;

    public TemplateBuilder(StreamDefinition streamDefinition, String template) {
        parse(streamDefinition, template);
    }

    public static Map<String, Object> convert(Event event, Map<String, TemplateBuilder> converterMap) {
        Map<String, Object> mapped = new HashMap<>();
        for (Map.Entry<String, TemplateBuilder> entry : converterMap.entrySet()) {
            mapped.put(entry.getKey(), entry.getValue().build(event));
        }
        return mapped;
    }

    public static Object[] convert(Event event, TemplateBuilder[] templateBuilders) {
        Object[] mapped = new String[templateBuilders.length];
        int i = 0;
        for (TemplateBuilder templateBuilder : templateBuilders) {
            mapped[i] = templateBuilder.build(event);
            i++;
        }
        return mapped;
    }

    public Object build(Event event) {
        if (isObjectMessage) {
            return event.getData()[objectIndex];
        } else {
            return formatMessage(event.getData());
        }

    }

    public Object build(ComplexEvent complexEvent) {
        if (isObjectMessage) {
            return complexEvent.getOutputData()[objectIndex];
        } else {
            return formatMessage(complexEvent.getOutputData());
        }

    }

    private void parse(StreamDefinition streamDefinition, String template) {

        if (Arrays.asList(streamDefinition.getAttributeNameArray()).contains(template.trim())) {
            this.objectIndex = streamDefinition.getAttributePosition(template.trim());
            this.isObjectMessage = true;
            this.type = streamDefinition.getAttributeList().get(objectIndex).getType();
        } else {
            if (template.matches("^`[^\\s]*`$")) {
                template = template.replaceAll("^`|`$", "");
            }

            String templateString = parseTextMessage(streamDefinition, template);
            String[] templateArray = templateString.split(SPLIT_PATTERN);
            assignTemplateArrayAttributePositions(templateArray, streamDefinition);
            this.splitTemplateArray = templateArray;
        }
    }

    private String parseTextMessage(StreamDefinition streamDefinition, String template) {
        // note: currently we do not support arbitrary data to be mapped with dynamic options
        List<String> attributes = Arrays.asList(streamDefinition.getAttributeNameArray());
        StringBuffer result = new StringBuffer();
        Matcher m = DYNAMIC_PATTERN.matcher(template);
        while (m.find()) {
            if (m.group(1) != null) {
                int attrIndex = attributes.indexOf(m.group(1).replaceAll("\\p{Ps}", "").replaceAll("\\p{Pe}", ""));
                if (attrIndex >= 0) {
                    m.appendReplacement(result, String.format("{.{%s}.}", attrIndex));
                } else {
                    throw new NoSuchAttributeException(String.format("Attribute : %s does not exist in %s.",
                            m.group(1), streamDefinition));
                }
            } else {
                m.appendReplacement(result, m.group() + "");
            }
        }
        m.appendTail(result);
        return result.toString();
    }

    private String formatMessage(Object[] outputData) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < splitTemplateArray.length; i++) {
            if (i % 2 == 0) {
                stringBuilder.append(splitTemplateArray[i]);
            } else {
                stringBuilder.append(outputData[positionArray[i / 2]]);
            }
        }
        return stringBuilder.toString();
    }

    private void assignTemplateArrayAttributePositions(String[] splitTemplateArray,
                                                       StreamDefinition streamDefinition) {
        this.positionArray = new int[splitTemplateArray.length / 2];
        int positionCount = 0;
        for (int i = 0; i < splitTemplateArray.length; i++) {
            if (i % 2 != 0) {
                try {
                    positionArray[positionCount++] = Integer.parseInt(splitTemplateArray[i]);
                } catch (NumberFormatException e) {
                    throw new SiddhiAppCreationException(String.format("Invalid mapping configuration provided in " +
                            "%s. Mapping parameter should be surrounded only with '{{' and '}}'.", streamDefinition));
                }
            }
        }
    }

    public Attribute.Type getType() {
        return type;
    }

    public boolean isObjectMessage() {
        return isObjectMessage;
    }
}
