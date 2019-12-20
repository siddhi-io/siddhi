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
package io.siddhi.query.api;

import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.exception.DuplicateAttributeException;
import io.siddhi.query.api.exception.DuplicateDefinitionException;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.testng.annotations.Test;

public class DefineTableTestCase {

    //define stream StockStream (symbol string, price int, volume float );

    @Test
    public void testCreatingTableDefinition() {
        SiddhiApp.siddhiApp("test").defineTable(TableDefinition.id("StockStream").attribute("symbol",
                Attribute.Type.STRING).attribute("price", Attribute.Type.INT).attribute("volume", Attribute.Type
                .FLOAT));
    }

    @Test(expectedExceptions = DuplicateAttributeException.class)
    public void testCreatingStreamWithDuplicateAttribute() {
        TableDefinition.id("StockStream").attribute("symbol", Attribute.Type.STRING).attribute("symbol", Attribute
                .Type.INT).attribute("volume", Attribute.Type.FLOAT);
    }

    @Test
    public void testCreatingSQLTableDefinition() {
        TableDefinition.id("StockStream").attribute("symbol", Attribute.Type.STRING).attribute("price", Attribute
                .Type.INT).attribute("volume", Attribute.Type.FLOAT).annotation(Annotation.annotation("From").element
                ("datasource.id", "cepDataSource"));
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void testTableDefinitionNull() {
        TableDefinition tableDefinition = null;
        SiddhiApp.siddhiApp("test").defineTable(tableDefinition);
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void testTableIdNull() {
        TableDefinition tableDefinition = TableDefinition.id(null);
        SiddhiApp.siddhiApp("test").defineTable(tableDefinition);
    }

    @Test(expectedExceptions = DuplicateDefinitionException.class)
    public void testCreatingTableDefinition2() {
        SiddhiApp.siddhiApp("test").defineTable(TableDefinition.id("StockStream").attribute("symbol",
                Attribute.Type.STRING).attribute("price", Attribute.Type.INT).attribute("volume", Attribute.Type
                .FLOAT)).defineTable(TableDefinition.id("StockStream"));
    }
}
