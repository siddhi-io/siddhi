/*
 *  Copyright (c) 2019 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package io.siddhi.query.api;

import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.definition.WindowDefinition;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.testng.annotations.Test;

public class DefineWindowTestCase {

    @Test
    public void testCreatingFunctionDefinition() {

        SiddhiApp.siddhiApp("Test").defineWindow(WindowDefinition.id("StockWindow")).
                defineStream(StreamDefinition.id("stockStream").attribute("symbol",
                        Attribute.Type.STRING).attribute("price", Attribute.Type.INT).attribute("volume", Attribute.Type
                        .FLOAT)).defineStream(StreamDefinition.id("StockStream"));
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void testCreatingFunctionDefinition3() {

        SiddhiApp.siddhiApp("Test").defineWindow(WindowDefinition.id(null)).
                defineStream(StreamDefinition.id("stockStream").attribute("symbol",
                        Attribute.Type.STRING).attribute("price", Attribute.Type.INT).attribute("volume", Attribute.Type
                        .FLOAT));
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void testCreatingFunctionDefinition4() {

        SiddhiApp.siddhiApp("Test").defineWindow(null).
                defineStream(StreamDefinition.id("stockStream").attribute("symbol",
                        Attribute.Type.STRING).attribute("price", Attribute.Type.INT).attribute("volume", Attribute.Type
                        .FLOAT));
    }
}
