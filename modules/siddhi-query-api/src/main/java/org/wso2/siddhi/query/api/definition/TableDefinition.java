/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.siddhi.query.api.definition;

import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.definition.io.Store;

public class TableDefinition extends AbstractDefinition {

    private Store store;

    protected TableDefinition(String id) {
        super(id);
    }

    public static TableDefinition id(String id) {
        return new TableDefinition(id);
    }

    public TableDefinition store(Store store){
        this.store = store;
        return this;
    }

    public Store getStore(){
        return store;
    }

    public TableDefinition attribute(String attributeName, Attribute.Type type) {
        checkAttribute(attributeName);
        this.attributeList.add(new Attribute(attributeName, type));
        return this;
    }

    public TableDefinition annotation(Annotation annotation) {
        annotations.add(annotation);
        return this;
    }

    @Override
    public String toString() {
        return "TableDefinition{" +
                "id='" + id + '\'' +
                ", attributeList=" + attributeList +
                ", annotations=" + annotations +
                ", store=" + store +
                '}';
    }

}
