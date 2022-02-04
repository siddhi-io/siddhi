/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.query.test;

import io.siddhi.query.compiler.SiddhiCompiler;
import io.siddhi.query.compiler.exception.SiddhiParserException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class VariableUpdateTestCase {

    @Test
    public void test1() throws SiddhiParserException {

        System.setProperty("foo", "Foo");
        String siddhiApp = "" +
                "@app:name('Test') " +
                "" +
                "define stream ${foo} (symbol string, price float, volume long);";
        String siddhiAppExpected = "" +
                "@app:name('Test') " +
                "" +
                "define stream Foo (symbol string, price float, volume long);";
        Assert.assertEquals(SiddhiCompiler.updateVariables(siddhiApp), siddhiAppExpected);
        System.clearProperty("foo");
    }

    @Test(expectedExceptions = SiddhiParserException.class)
    public void test2() throws SiddhiParserException {

        String siddhiApp = "" +
                "@app:name('Test') " +
                "" +
                "define stream ${foo2} (symbol string, price float, volume long);";
        SiddhiCompiler.updateVariables(siddhiApp);

    }

    @Test(expectedExceptions = SiddhiParserException.class)
    public void test3() throws SiddhiParserException {

        String siddhiApp =
                "define stream ${foo2} (symbol string, price float, volume long);";
        SiddhiCompiler.updateVariables(siddhiApp);
    }

    @Test
    public void test4() throws SiddhiParserException {

        System.setProperty("JAVA_HOME", "Foo");
        String siddhiApp = "" +
                "@app:name('Test') " +
                "" +
                "define stream ${JAVA_HOME} (symbol string, price float, volume long);";
        String siddhiAppExpected = "" +
                "@app:name('Test') " +
                "" +
                "define stream Foo (symbol string, price float, volume long);";
        Assert.assertEquals(SiddhiCompiler.updateVariables(siddhiApp), siddhiAppExpected);
        System.clearProperty("JAVA_HOME");
    }

    @Test
    public void test5() throws SiddhiParserException {

        String siddhiApp = "" +
                "@app:name('Test') " +
                "" +
                "define stream ${JAVA_HOME} (symbol string, price float, volume long);";
        String siddhiAppExpected = "" +
                "@app:name('Test') " +
                "" +
                "define stream Foo (symbol string, price float, volume long);";
        Assert.assertEquals(SiddhiCompiler.updateVariables(siddhiApp),
                siddhiAppExpected.replaceFirst("Foo", System.getenv("JAVA_HOME")));
    }
}
