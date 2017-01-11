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

package org.wso2.siddhi.query.test;

import org.junit.Assert;
import org.junit.Test;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.exception.DuplicateAttributeException;
import org.wso2.siddhi.query.compiler.SiddhiCompiler;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;

public class DefineStreamTestCase {

    @Test
    public void Test1() throws SiddhiParserException {
        StreamDefinition streamDefinition = SiddhiCompiler.parseStreamDefinition("define stream cseStream ( symbol string, price int, volume float )");
        Assert.assertEquals(StreamDefinition.
                        id("cseStream").
                        attribute("symbol", Attribute.Type.STRING).
                        attribute("price", Attribute.Type.INT).
                        attribute("volume", Attribute.Type.FLOAT).toString(),
                streamDefinition.toString());
    }

    @Test
    public void Test2() throws SiddhiParserException {
        StreamDefinition streamDefinition = SiddhiCompiler.parseStreamDefinition("define stream `define` ( `string` string, price int, volume float );");
        Assert.assertEquals(StreamDefinition.
                        id("define").
                        attribute("string", Attribute.Type.STRING).
                        attribute("price", Attribute.Type.INT).
                        attribute("volume", Attribute.Type.FLOAT).toString(),
                streamDefinition.toString());
    }

    //define stream StockStream (symbol string, price int, volume float );

    @Test
    public void testCreatingStreamDefinition() {
        StreamDefinition streamDefinition = SiddhiCompiler.parseStreamDefinition("define stream StockStream ( symbol string, price int, volume float );");
        StreamDefinition api = StreamDefinition.id("StockStream").attribute("symbol", Attribute.Type.STRING).attribute("price", Attribute.Type.INT).attribute("volume", Attribute.Type.FLOAT);
        Assert.assertEquals(api, streamDefinition);
    }

    @Test(expected = DuplicateAttributeException.class)
    public void testCreatingStreamWithDuplicateAttribute() {
        StreamDefinition streamDefinition = SiddhiCompiler.parseStreamDefinition("define stream StockStream ( symbol string, symbol int, volume float );");
//        StreamDefinition.id("StockStream").attribute("symbol", Attribute.Type.STRING).attribute("symbol", Attribute.Type.INT).attribute("volume", Attribute.Type.FLOAT);
    }

    @Test
    public void testCreatingStreamDefinition2() {
        StreamDefinition streamDefinition = SiddhiCompiler.parseStreamDefinition("define stream StockStream ( symbol string, price int, volume double, data Object );");
        StreamDefinition api = StreamDefinition.id("StockStream").attribute("symbol", Attribute.Type.STRING).attribute("price", Attribute.Type.INT).attribute("volume", Attribute.Type.DOUBLE).attribute("data", Attribute.Type.OBJECT);
        Assert.assertEquals(api, streamDefinition);
    }

    @Test
    public void testEqualObjects() throws SiddhiParserException {
        StreamDefinition streamDefinition = SiddhiCompiler.parseStreamDefinition("@Foo(name='bar','Custom')define stream cseStream ( symbol string, price int, volume float )");
        Assert.assertEquals(StreamDefinition.
                        id("cseStream").
                        attribute("symbol", Attribute.Type.STRING).
                        attribute("price", Attribute.Type.INT).
                        attribute("volume", Attribute.Type.FLOAT).annotation(Annotation.annotation("Foo").element("name", "bar").element("Custom")),
                streamDefinition);
    }
}
