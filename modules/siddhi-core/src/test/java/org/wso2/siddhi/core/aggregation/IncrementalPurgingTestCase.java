package org.wso2.siddhi.core.aggregation;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

public class IncrementalPurgingTestCase {
    private static final Logger LOG = Logger.getLogger(IncrementalPurgingTestCase.class);

    @Test
    public void incrementalPurgingTest1() {
        LOG.info("incrementalPurgingTest1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream = "" +
                " define stream stockStream (arrival long, symbol string, price float, volume int); ";

        String query = "" +
                " @info(name = 'query1') " +
                " @purge(enable='true',interval='1 min',@retentionPeriod(sec='30 sec',min='24 h',hours='30 day'" +
                ",days='5 year',months='10 months',years='5 years'))" +
                " define aggregation stockAggregation " +
                " from stockStream " +
                " select sum(price) as sumPrice " +
                " aggregate by arrival every sec ... min";

        siddhiManager.createSiddhiAppRuntime(stockStream + query);
    }

    @Test(dependsOnMethods = "incrementalPurgingTest1")
    public void incrementalPurgingTest2() throws InterruptedException {
        LOG.info("incrementalPurgingTest2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int);";
        String query = "  @purge(enable='true',interval='30 sec',@retentionPeriod(sec='10 sec',min='all',hours='all'" +
                "                ,days='all',months='all',years='all'))  " +
                "define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, avg(price) as avgPrice, sum(price) as totalPrice, (price * quantity) " +
                "as lastTradeValue  " +
                "group by symbol " +
                "aggregate every sec...hour ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);

        InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
        siddhiAppRuntime.start();

        stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6});
        Thread.sleep(2000);
        stockStreamInputHandler.send(new Object[]{"WSO2", 70f, null, 40L, 10});
        Thread.sleep(2000);

        stockStreamInputHandler.send(new Object[]{"WSO2", 60f, 44f, 200L, 56});
        Thread.sleep(2000);

        stockStreamInputHandler.send(new Object[]{"WSO2", 100f, null, 200L, 16});
        Thread.sleep(2000);

        stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 26});
        Thread.sleep(2000);

        stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 96});
        Thread.sleep(2000);

        Thread.sleep(100);

        Event[] events = siddhiAppRuntime.query("from stockAggregation " +
                "within \"2018-07-** **:**:**\" " +
                "per \"seconds\"");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(6, events.length);
        Thread.sleep(40000);
        events = siddhiAppRuntime.query("from stockAggregation " +
                "within \"2018-07-** **:**:**\" " +
                "per \"seconds\"");
        AssertJUnit.assertNull(events);
        siddhiAppRuntime.shutdown();

    }
    @Test(dependsOnMethods = "incrementalPurgingTest2")
    public void incrementalPurgingTest3() throws InterruptedException {
        LOG.info("incrementalPurgingTest3");
        SiddhiManager siddhiManager = new SiddhiManager();

        String stockStream =
                "define stream stockStream (symbol string, price float, lastClosingPrice float, volume long , " +
                        "quantity int);";
        String query = "  @purge(enable='true',interval='90 sec',@retentionPeriod(sec='all',min='1 sec',hours='all'" +
                "                ,days='all',months='all',years='all'))  " +
                "define aggregation stockAggregation " +
                "from stockStream " +
                "select symbol, avg(price) as avgPrice, sum(price) as totalPrice, (price * quantity) " +
                "as lastTradeValue  " +
                "group by symbol " +
                "aggregate every sec...hour ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(stockStream + query);

        InputHandler stockStreamInputHandler = siddhiAppRuntime.getInputHandler("stockStream");
        siddhiAppRuntime.start();

        stockStreamInputHandler.send(new Object[]{"WSO2", 50f, 60f, 90L, 6});
        Thread.sleep(2000);
        stockStreamInputHandler.send(new Object[]{"WSO2", 70f, null, 40L, 10});
        Thread.sleep(2000);

        stockStreamInputHandler.send(new Object[]{"WSO2", 60f, 44f, 200L, 56});
        Thread.sleep(2000);

        stockStreamInputHandler.send(new Object[]{"WSO2", 100f, null, 200L, 16});
        Thread.sleep(2000);

        stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 26});
        Thread.sleep(2000);

        stockStreamInputHandler.send(new Object[]{"IBM", 100f, null, 200L, 96});
        Thread.sleep(2000);

        Thread.sleep(70000);

        Event[] events = siddhiAppRuntime.query("from stockAggregation " +
                "within \"2018-07-** **:**:**\" " +
                "per \"minutes\"");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(2, events.length);
        Thread.sleep(20000);
        events = siddhiAppRuntime.query("from stockAggregation " +
                "within \"2018-07-** **:**:**\" " +
                "per \"minutes\"");
        AssertJUnit.assertNull(events);
        siddhiAppRuntime.shutdown();

    }
}
