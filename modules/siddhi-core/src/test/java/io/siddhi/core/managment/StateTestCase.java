/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.query.QueryRuntime;
import io.siddhi.core.table.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StateTestCase {
    private static final Logger log = LogManager.getLogger(StateTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;

    @BeforeMethod
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }

    @Test
    public void stateTest1() throws InterruptedException {
        log.info("stateTest1");

        SiddhiManager siddhiManager = new SiddhiManager();

        String app = "" +
                "define stream cseEventStream (symbol string, price float, volume int);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream " +
                "select * " +
                "insert all events into outputStream;" +
                "" +
                "@info(name = 'query2') " +
                "from cseEventStream#window.timeBatch(1 sec) " +
                "select * " +
                "insert all events into outputStream;" +
                "" +
                "@info(name = 'query3') " +
                "from cseEventStream " +
                "select sum(price) as total " +
                "insert all events into outputStream1;" +
                "" +
                "@info(name = 'query4') " +
                "from cseEventStream " +
                "select * " +
                "output every 5 min " +
                "insert all events into outputStream;" +
                "" +
                "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);

        for (QueryRuntime queryRuntime : siddhiAppRuntime.getQueries()) {
            inEventCount++;
            switch (inEventCount) {
                case 1:
                    Assert.assertFalse(queryRuntime.isStateful());
                    break;
                case 2:
                    Assert.assertTrue(queryRuntime.isStateful());
                    break;
                case 3:
                    Assert.assertTrue(queryRuntime.isStateful());
                    break;
                case 4:
                    Assert.assertTrue(queryRuntime.isStateful());
                    break;
            }
        }
        Assert.assertEquals(inEventCount, 4);
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void stateTest2() throws InterruptedException {
        log.info("stateTest2");

        SiddhiManager siddhiManager = new SiddhiManager();

        String app = "" +
                "define stream cseEventStream (symbol string, price float, volume int);" +
                "" +
                "partition with (symbol of cseEventStream) " +
                "begin " +
                "" +
                "   @info(name = 'query1') " +
                "   from cseEventStream " +
                "   select * " +
                "   insert all events into outputStream;" +
                "" +
                "   @info(name = 'query2') " +
                "   from cseEventStream#window.timeBatch(1 sec) " +
                "   select * " +
                "   insert all events into outputStream;" +
                "" +
                "   @info(name = 'query3') " +
                "   from cseEventStream " +
                "   select sum(price) as total " +
                "   insert all events into outputStream1;" +
                "" +
                "   @info(name = 'query4') " +
                "   from cseEventStream " +
                "   select * " +
                "   output every 5 min " +
                "   insert all events into outputStream;" +
                "" +
                "end " +
                "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);

        for (QueryRuntime queryRuntime : siddhiAppRuntime.getPartitions().iterator().next().getQueries()) {
            inEventCount++;
            switch (inEventCount) {
                case 1:
                    Assert.assertFalse(queryRuntime.isStateful());
                    break;
                case 2:
                    Assert.assertTrue(queryRuntime.isStateful());
                    break;
                case 3:
                    Assert.assertTrue(queryRuntime.isStateful());
                    break;
                case 4:
                    Assert.assertTrue(queryRuntime.isStateful());
                    break;
            }
        }
        Assert.assertEquals(inEventCount, 4);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void stateTest3() throws InterruptedException {
        log.info("stateTest3");

        SiddhiManager siddhiManager = new SiddhiManager();

        String app = "" +
                "" +
                "define table cseEventTable (symbol string, price float, volume int);" +
                "" +
                "@Store(type='testStoreContainingInMemoryTable') " +
                "define table StockTable (symbol string, price float, volume long); " +
                "" +
                "define window cseEventWindow (symbol string, price float, volume int) time(1 sec) " +
                "       output all events; " +
                "" +
                "define trigger triggerStream at every 500 milliseconds ;" +
                "" +
                "define stream cseEventStream (symbol string, price float, volume int);" +
                "define stream twitterStream (user string, tweet string, company string); " +
                "define stream Stream1 (symbol string, price float, volume int); " +
                "define stream Stream2 (symbol string, price float, volume int); " +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream#window.time(1 sec) join twitterStream#window.time(1 sec) " +
                "on cseEventStream.symbol== twitterStream.company " +
                "select cseEventStream.symbol as symbol, twitterStream.tweet, cseEventStream.price " +
                "insert all events into outputStream ;" +
                "" +
                "@info(name = 'query2') " +
                "from every ( e1=Stream1[price > 20] -> e2=Stream2[price > e1.price] " +
                "   or e3=Stream2['IBM' == symbol]) -> e4=Stream2[price > e1.price] " +
                "select e1.price as price1, e2.price as price2, e3.price as price3, e4.price as price4 " +
                "insert into OutputStream ;" +
                "";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(app);

        for (QueryRuntime queryRuntime : siddhiAppRuntime.getQueries()) {
            inEventCount++;
            switch (inEventCount) {
                case 1:
                    Assert.assertTrue(queryRuntime.isStateful());
                    break;
                case 2:
                    Assert.assertTrue(queryRuntime.isStateful());
                    break;
            }
        }

        for (Table table : siddhiAppRuntime.getTables()) {
            inEventCount++;
            switch (inEventCount) {
                case 3:
                    Assert.assertTrue(table.isStateful());
                    break;
                case 4:
                    Assert.assertFalse(table.isStateful());
                    break;
            }
        }

        inEventCount++;
        Assert.assertTrue(siddhiAppRuntime.getWindows().iterator().next().isStateful());

        inEventCount++;
        Assert.assertFalse(siddhiAppRuntime.getTiggers().iterator().next().isStateful());

        Assert.assertEquals(inEventCount, 6);
        siddhiAppRuntime.shutdown();

    }
}

