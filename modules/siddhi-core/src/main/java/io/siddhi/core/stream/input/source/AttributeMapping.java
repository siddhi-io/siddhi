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

package io.siddhi.core.stream.input.source;

import io.siddhi.query.api.definition.Attribute;

/**
 * Holder object to store mapping information for a given Siddhi Attribute
 * {@link io.siddhi.query.api.definition.Attribute}
 */
public class AttributeMapping {
    protected String name;
    protected int position;
    protected Attribute.Type type;
    private String mapping = null;

    public AttributeMapping(String name, int position, String mapping, Attribute.Type type) {
        this.name = name;
        this.position = position;
        this.mapping = mapping;
        this.type = type;
    }

    public String getMapping() {
        return mapping;
    }

    public String getName() {
        return name;
    }

    public int getPosition() {
        return position;
    }

    public Attribute.Type getType() {
        return type;
    }

    @Override
    public String toString() {
        return "AttributeMapping{" +
                "name='" + name + '\'' +
                ", position=" + position +
                ", mapping='" + mapping + '\'' +
                ", type=" + type +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AttributeMapping)) {
            return false;
        }

        AttributeMapping that = (AttributeMapping) o;

        if (position != that.position) {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (mapping != null ? !mapping.equals(that.mapping) : that.mapping != null) {
            return false;
        }
        return type == that.type;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + position;
        result = 31 * result + (mapping != null ? mapping.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        return result;
    }
}
