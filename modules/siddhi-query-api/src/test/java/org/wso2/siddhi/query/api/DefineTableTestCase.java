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
package org.wso2.siddhi.query.api;

import org.junit.Test;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.TableDefinition;
import org.wso2.siddhi.query.api.definition.io.Store;
import org.wso2.siddhi.query.api.exception.DuplicateAttributeException;

public class DefineTableTestCase {

    //define stream StockStream (symbol string, price int, volume float );

    @Test
    public void testCreatingTableDefinition() {
        ExecutionPlan.executionPlan("test").defineTable(TableDefinition.id("StockStream").attribute("symbol", Attribute.Type.STRING).attribute("price", Attribute.Type.INT).attribute("volume", Attribute.Type.FLOAT));
    }

    @Test(expected = DuplicateAttributeException.class)
    public void testCreatingStreamWithDuplicateAttribute() {
        TableDefinition.id("StockStream").attribute("symbol", Attribute.Type.STRING).attribute("symbol", Attribute.Type.INT).attribute("volume", Attribute.Type.FLOAT);
    }

    @Test
    public void testCreatingSQLTableDefinition() {
        TableDefinition.id("StockStream").attribute("symbol", Attribute.Type.STRING).attribute("price", Attribute.Type.INT).attribute("volume", Attribute.Type.FLOAT).annotation(Annotation.annotation("From").element("datasource.id", "cepDataSource"));
    }

    @Test
    public void testCreatingTableWithStoreDefinition() {
        /*define table FooTable (time long, data string)
        store rdbms options (url “http://localhost:8900”,
        username “test”) ;*/
        Store store = Store.store("rdbms").
                option("url", "http://localhost:8900").
                option("username", "test");

        TableDefinition.id("FooTable").
                attribute("time", Attribute.Type.STRING).
                attribute("data", Attribute.Type.STRING).store(store);
    }
}
