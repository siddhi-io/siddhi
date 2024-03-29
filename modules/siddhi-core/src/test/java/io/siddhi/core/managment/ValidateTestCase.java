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

package io.siddhi.core.managment;

import io.siddhi.core.SiddhiManager;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.query.compiler.exception.SiddhiParserException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ValidateTestCase {
    private static final Logger log = LogManager.getLogger(ValidateTestCase.class);
    private int count;
    private boolean eventArrived;
    private int inEventCount;
    private int removeEventCount;

    @BeforeMethod
    public void init() {
        count = 0;
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }


    @Test
    public void validateTest1() throws InterruptedException {
        log.info("validate test1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "@app:name('validateTest') " +
                "" +
                "define stream cseEventStream (symbol string, price float, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[symbol is null] " +
                "select symbol, price " +
                "insert into outputStream;";

        siddhiManager.validateSiddhiApp(siddhiApp);

    }


    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void validateTest2() throws InterruptedException {
        log.info("validate test2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String siddhiApp = "" +
                "@app:name('validateTest') " +
                "" +
                "define stream cseEventStream (symbol string, price float, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStreamA[symbol is null] " +
                "select symbol, price " +
                "insert into outputStream;";


        siddhiManager.validateSiddhiApp(siddhiApp);

    }

    @Test
    public void validateTest3() throws InterruptedException {
        log.info("validate test3");
        SiddhiManager siddhiManager = new SiddhiManager();

        System.setProperty("stream", "cseEventStream");

        String siddhiApp = "" +
                "@app:name('validateTest') " +
                "" +
                "define stream ${stream} (symbol string, price float, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream " +
                "select symbol, price, '${JAVA_HOME}' as javaHome " +
                "insert into outputStream;";

        siddhiManager.validateSiddhiApp(siddhiApp);
        System.clearProperty("stream");

    }

    @Test(expectedExceptions = SiddhiParserException.class)
    public void validateTest4() throws InterruptedException {
        log.info("validate test4");
        SiddhiManager siddhiManager = new SiddhiManager();

        System.setProperty("streams", "cseEventStream");
        try {
            String siddhiApp = "" +
                    "@app:name('validateTest') " +
                    "" +
                    "define stream ${stream} (symbol string, price float, volume long);" +
                    "" +
                    "@info(name = 'query1') " +
                    "from cseEventStream " +
                    "select symbol, price " +
                    "insert into outputStream;";

            siddhiManager.validateSiddhiApp(siddhiApp);
        } finally {
            System.clearProperty("streams");

        }

    }
}
