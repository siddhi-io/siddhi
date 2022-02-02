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

import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.FunctionDefinition;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DefineFunctionTestCase {

    @Test
    public void testFunction1() {

        SiddhiApp siddhiApp = SiddhiApp.siddhiApp("test").defineFunction(
                new FunctionDefinition().id("foo").language("JS").type(Attribute.Type.STRING)
                        .body("return 'hello world!'"));

        SiddhiApp siddhiApp1 = SiddhiApp.siddhiApp("test").defineFunction(
                new FunctionDefinition().id("foo").language("JS").type(Attribute.Type.STRING)
                        .body("return 'hello world!'"));

        Assert.assertTrue(siddhiApp.getFunctionDefinitionMap().containsKey("foo"));
        Assert.assertTrue(siddhiApp.equals(siddhiApp1));
        Assert.assertEquals(siddhiApp.hashCode(), siddhiApp1.hashCode());
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void testFunction2() {

        SiddhiApp.siddhiApp("test").defineFunction(
                new FunctionDefinition().id("foo").type(Attribute.Type.STRING)
                        .body("return 'hello world!'"));

    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void testFunction3() {

        SiddhiApp.siddhiApp("test").defineFunction(
                new FunctionDefinition().id("foo").language("JS")
                        .body("return 'hello world!'"));

    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void testFunction4() {

        SiddhiApp.siddhiApp("test").defineFunction(
                new FunctionDefinition().language("JS").type(Attribute.Type.STRING)
                        .body("return 'hello world!'"));

    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void testFunction5() {

        SiddhiApp.siddhiApp("test").defineFunction(
                new FunctionDefinition().id("foo").language("JS").type(Attribute.Type.STRING));

    }
}
