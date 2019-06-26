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
package io.siddhi.query.api.definition;

import io.siddhi.query.api.annotation.Annotation;

/**
 * Siddhi Stream Definition
 */
public class StreamDefinition extends AbstractDefinition {

    private static final long serialVersionUID = 1L;
    protected String[] attributeNameArray;
    protected boolean hasDefinitionChanged = false;
    
    public StreamDefinition() {
    }

    protected StreamDefinition(String streamId) {
        super(streamId);
    }

    public static StreamDefinition id(String streamId) {
        return new StreamDefinition(streamId);
    }

    public StreamDefinition attribute(String attributeName, Attribute.Type type) {
        checkAttribute(attributeName);
        this.attributeList.add(new Attribute(attributeName, type));
        this.hasDefinitionChanged = true;
        return this;
    }
    
    // overriding the base implementation to remove hotspot on this method call
    // iterating the attribute list only if there is a change
    @Override
    public String[] getAttributeNameArray() {
        if (hasDefinitionChanged) {
            int attributeListSize = attributeList.size();
            this.attributeNameArray = new String[attributeListSize];
            for (int i = 0; i < attributeListSize; i++) {
                this.attributeNameArray[i] = attributeList.get(i).getName();
            }

            hasDefinitionChanged = false;
        }

        return this.attributeNameArray;
    }

    public StreamDefinition annotation(Annotation annotation) {
        annotations.add(annotation);
        return this;
    }

    public StreamDefinition clone() {
        StreamDefinition streamDefinition = new StreamDefinition(this.id);
        for (Attribute attribute: this.attributeList) {
            streamDefinition.attribute(attribute.getName(), attribute.getType());
        }
        for (Annotation annotation: this.annotations) {
            streamDefinition.annotation(annotation);
        }
        return streamDefinition;
    }

}
