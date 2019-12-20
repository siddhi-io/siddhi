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
 * Siddhi Table Definition.
 */
public class TableDefinition extends AbstractDefinition {
    private static final long serialVersionUID = 1L;

    protected TableDefinition(String id) {
        super(id);
    }

    public static TableDefinition id(String id) {
        return new TableDefinition(id);
    }

    public TableDefinition attribute(String attributeName, Attribute.Type type) {
        super.attribute(attributeName, type);
        return this;
    }

    public TableDefinition annotation(Annotation annotation) {
        annotations.add(annotation);
        return this;
    }

    @Override
    public String toString() {
        return super.toString("table");
    }

}
