/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.core.query.pattern.absent;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;

/**
 * Test the ANTLR parser of absent pattern.
 */
public class AbsentPatternParserTestCase {

    private static final Logger log = Logger.getLogger(AbsentPatternParserTestCase.class);

    @Test(expected = SiddhiParserException.class)
    public void testQueryParser1() throws InterruptedException {
        log.info("Test the query 'e1 -> not e2' parser of pattern for absence of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] -> not Stream2[price>e1.price] " +
                "select e1.symbol as symbol, e1.price as price " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryParser2() throws InterruptedException {
        log.info("Test the query 'e1 -> not e2 for 2 sec' parser of pattern for absence of events with select e2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] -> not Stream2[price>e1.price] for 2 sec " +
                "select e1.symbol as symbol1 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryParser3() throws InterruptedException {
        log.info("Test the query 'e1 -> not e2 for 2 sec' parser of pattern for absence of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] -> not Stream2[price>e1.price] for 2 sec " +
                "select e1.symbol as symbol, e1.price as price " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }


    @Test
    public void testQueryParser4() throws InterruptedException {
        log.info("Test the query 'every e1 -> not e2 for 2 sec' parser of pattern for absence of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from every e1=Stream1[price>20] -> not Stream2[price>e1.price] for 2 sec " +
                "select e1.symbol as symbol, e1.price as price " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }

    @Test(expected = SiddhiParserException.class)
    public void testQueryParser5() throws InterruptedException {
        log.info("Test the query 'every e1 -> not e2' parser of pattern for absence of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from every e1=Stream1[price>20] -> not Stream2[price>e1.price] " +
                "select e1.symbol as symbol, e1.price as price " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }

    @Test(expected = SiddhiParserException.class)
    public void testQueryParser6() throws InterruptedException {
        log.info("Test the query 'not e1 -> not e2 for 2 sec' parser of pattern for absence of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>20] -> not Stream2[price>30] for 2 sec " +
                "select 25.0f as price " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryParser7() throws InterruptedException {
        log.info("Test the query 'not e1 -> not e2 for 2 sec -> e3' parser of pattern for absence of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] -> not Stream2[price>e1.price] for 2 sec -> " +
                "e3=Stream3[symbol==e1.symbol] " +
                "select e1.symbol as symbol, e1.price as price " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryParser8() throws InterruptedException {
        log.info("Test the query 'e1 -> e3 -> not e2 for 2 sec' parser of pattern for absence of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "define stream Stream3 (symbol string, price float, volume int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price>20] -> e3=Stream3[symbol==e1.symbol] -> not Stream2[price>e1.price] for 2 " +
                "sec " +
                "select e1.symbol as symbol, e3.price as price " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryParser9() throws InterruptedException {
        log.info("Test the query 'not e1 for 2 sec -> e2' parser of pattern for absence of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>20] for 2 sec -> e2=Stream2[symbol=='IBM'] " +
                "select e2.price as price " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryParser10() throws InterruptedException {
        log.info("Test the query 'every not e1 for 2 sec -> e2' parser of pattern for absence of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from every not Stream1[price>20] for 2 sec -> e2=Stream2[symbol=='IBM'] " +
                "select e2.price as price " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }

    @Test(expected = SiddhiParserException.class)
    public void testQueryParser11() throws InterruptedException {
        log.info("Test the query 'not e1 for 2 sec -> not e2 for 2 sec' parser of pattern for absence of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from not e1=Stream1[price>20] for 2 sec -> not Stream2[price>e1.price] for 2 sec " +
                "select e1.symbol as symbol, e1.price as price " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }

    @Test
    public void testQueryParser12() throws InterruptedException {
        log.info("Test the query 'e1 -> not e2 for 2 sec and e3' parser of pattern for absence of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from e1=Stream1[price > 20] -> not Stream2[price > e1.price] and e3=Stream2['IBM' == " +
                "symbol] " +
                "select e1.symbol as symbol1, e3.price as price3 " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }

    @Test(expected = SiddhiParserException.class)
    public void testQueryParser13() throws InterruptedException {
        log.info("Test the query 'not e1 for 2 sec -> not e2 for 2 sec' parser of pattern for absence of events");

        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from not Stream1[price>20] for 2 sec -> not Stream2[price>e1.price] for 2 sec " +
                "select e1.symbol as symbol, e1.price as price " +
                "insert into OutputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);

        executionPlanRuntime.shutdown();
    }
}
