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
import io.siddhi.query.api.annotation.Element;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.exception.AttributeNotExistException;
import io.siddhi.query.api.exception.DuplicateAttributeException;
import io.siddhi.query.api.exception.DuplicateDefinitionException;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.util.Arrays;

public class DefineStreamTestCase {

    //define stream StockStream (symbol string, price int, volume float );

    @Test
    public void testCreatingStreamDefinition() {

        SiddhiApp.siddhiApp("Test").defineStream(StreamDefinition.id("StockStream").attribute("symbol",
                Attribute.Type.STRING).attribute("price", Attribute.Type.INT).attribute("volume", Attribute.Type
                .FLOAT));

    }

    @Test(expectedExceptions = DuplicateAttributeException.class)
    public void testCreatingStreamWithDuplicateAttribute() {

        StreamDefinition.id("StockStream").attribute("symbol", Attribute.Type.STRING).attribute("symbol", Attribute
                .Type.INT).attribute("volume", Attribute.Type.FLOAT);

    }

    @Test
    public void testCreatingStreamDefinitionWithObject() {

        StreamDefinition.id("StockStream").attribute("symbol", Attribute.Type.STRING).attribute("price", Attribute
                .Type.INT).attribute("volume", Attribute.Type.FLOAT).attribute("data", Attribute.Type.OBJECT);
    }

    @Test
    public void testAnnotatingStreamDefinition() {

        SiddhiApp.siddhiApp("Test").defineStream(StreamDefinition.id("StockStream").attribute("symbol",
                Attribute.Type.STRING).attribute("price", Attribute.Type.INT).attribute("volume", Attribute.Type
                .FLOAT).annotation(Annotation.annotation("distribute").element("true")));

    }

    @Test
    public void testAttribute() {

        StreamDefinition streamDefinition = StreamDefinition.id("StockStream").attribute("symbol", Attribute.Type
                .STRING).attribute("price", Attribute.Type.INT).attribute("volume", Attribute.Type.FLOAT);
        AssertJUnit.assertEquals(1, streamDefinition.getAttributePosition("price"));
        AssertJUnit.assertEquals(Attribute.Type.FLOAT, streamDefinition.getAttributeType("volume"));
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void testStreamdefintionNull() {

        StreamDefinition streamDefinition = null;
        SiddhiApp.siddhiApp("Test").defineStream(streamDefinition);
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void testStreamIdNull() {

        StreamDefinition streamDefinition = StreamDefinition.id(null);
        SiddhiApp.siddhiApp("Test").defineStream(streamDefinition);
    }

    @Test(expectedExceptions = DuplicateDefinitionException.class)
    public void testCreatingStreamDefinition2() {

        SiddhiApp.siddhiApp("Test").defineStream(StreamDefinition.id("StockStream").attribute("symbol",
                Attribute.Type.STRING).attribute("price", Attribute.Type.INT).attribute("volume", Attribute.Type
                .FLOAT)).defineStream(StreamDefinition.id("StockStream").attribute("index",
                Attribute.Type.INT).attribute("price", Attribute.Type.INT).attribute("volume", Attribute.Type
                .FLOAT));
    }

    @Test
    public void testAnnotatingStreamDefinition2() {

        Annotation annotation = AbstractDefinition.annotation("distribute");

        StreamDefinition streamDefinition = StreamDefinition.id("StockStream").attribute("symbol",
                Attribute.Type.STRING).attribute("price", Attribute.Type.INT).attribute("volume", Attribute.Type
                .FLOAT).annotation(annotation.element("true"));

        String annotationString = "[@distribute( \"true\")]";

        SiddhiApp.siddhiApp("Test").defineStream(streamDefinition);

        AssertJUnit.assertEquals(annotationString, streamDefinition.getAnnotations().toString());
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void testStreamDefinition() {

        StreamDefinition streamDefinition = new StreamDefinition();

        SiddhiApp.siddhiApp("Test").defineStream(streamDefinition);
    }

    @Test(expectedExceptions = AttributeNotExistException.class)
    public void testStreamDefinition2() {

        StreamDefinition streamDefinition = StreamDefinition.id("StockStream").attribute("symbol", Attribute.Type
                .STRING).attribute("price", Attribute.Type.INT).attribute("volume", Attribute.Type.FLOAT);
        streamDefinition.getAttributeType("stockprice");
    }

    @Test(expectedExceptions = AttributeNotExistException.class)
    public void testStreamDefinition3() {

        StreamDefinition streamDefinition = StreamDefinition.id("StockStream").attribute("symbol", Attribute.Type
                .STRING).attribute("price", Attribute.Type.INT).attribute("volume", Attribute.Type.FLOAT);
        streamDefinition.getAttributePosition("stockprice");
    }

    @Test
    public void testStreamDefinition4() {

        String attributeList = "[Attribute{id='symbol', type=STRING}, Attribute{id='price', type=INT}," +
                " Attribute{id='volume', type=FLOAT}]";

        StreamDefinition streamDefinition = StreamDefinition.id("StockStream").attribute("symbol", Attribute.Type
                .STRING).attribute("price", Attribute.Type.INT).attribute("volume", Attribute.Type.FLOAT);

        AssertJUnit.assertEquals(attributeList, streamDefinition.getAttributeList().toString());
    }

    @Test
    public void testStreamDefinition5() {

        StreamDefinition streamDefinition = StreamDefinition.id("StockStream").attribute("symbol", Attribute.Type
                .STRING).attribute("price", Attribute.Type.INT).attribute("volume", Attribute.Type.FLOAT);
        String[] list = new String[]{"symbol", "price", "volume"};

        AssertJUnit.assertEquals(Arrays.toString(list), Arrays.toString(streamDefinition.getAttributeNameArray()));
    }

    @Test
    public void testAnnotatingStreamDefinition3() {

        Element element = new Element("name", "query1");

        String annotationString = "[@map( type = \"xml\", namespace = \"h=uri, a=uri\", @attributes( \"//h:time\", " +
                "\"//h:data\"))]";
        String elementString = "name = \"query1\"";

        Annotation annotation = Annotation.annotation("source")
                .element("type", "http")
                .element("context", "/test")
                .element("transport", "http,https")
                .annotation(Annotation.annotation("map")
                        .element("type", "xml")
                        .element("namespace", "h=uri, a=uri")
                        .annotation(Annotation.annotation("attributes")
                                .element("//h:time")
                                .element("//h:data")
                        )
                );
        StreamDefinition streamDefinition = StreamDefinition.id("StockStream").attribute("symbol",
                Attribute.Type.STRING).attribute("price", Attribute.Type.INT).attribute("volume", Attribute.Type
                .FLOAT).annotation(annotation.element(element));

        annotation.setName("sink");

        AssertJUnit.assertEquals(annotationString, annotation.getAnnotations().toString());
        AssertJUnit.assertEquals(annotationString, annotation.getAnnotations("map").toString());
        AssertJUnit.assertEquals("sink", annotation.getName());
        AssertJUnit.assertEquals("http", annotation.getElement("type"));
        AssertJUnit.assertEquals("name", element.getKey());
        AssertJUnit.assertEquals("query1", element.getValue());
        AssertJUnit.assertEquals(elementString, element.toString());

    }

    @Test
    public void testStreamDefinition6() {

        StreamDefinition streamDefinition = StreamDefinition.id("Foo");
        streamDefinition.setId("StockStream");
        streamDefinition.attribute("symbol", Attribute.Type
                .STRING).attribute("price", Attribute.Type.INT).attribute("volume", Attribute.Type.FLOAT);
        StreamDefinition streamDefinition2 = StreamDefinition.id("StockStream").attribute("symbol", Attribute.Type
                .STRING).attribute("price", Attribute.Type.INT).attribute("volume", Attribute.Type.FLOAT);
        Assert.assertEquals(streamDefinition, streamDefinition2);
        Assert.assertEquals(streamDefinition.hashCode(), streamDefinition2.hashCode());
        streamDefinition.annotation(Annotation.annotation("Foo"));
        Assert.assertTrue(streamDefinition.equalsIgnoreAnnotations(streamDefinition2));
        Assert.assertFalse(streamDefinition.equals(streamDefinition2));
    }
}
