package io.siddhi.core.query.table.cache;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.table.util.TestAppenderToValidateLogsForCachingTests;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CacheExpiryAndReloadTestCase {
    private static final Logger log = (Logger) LogManager.getLogger(CacheExpiryAndReloadTestCase.class);

    @Test
    public void expiryAndReloadTest0() throws InterruptedException, SQLException {
        log.info("expiryAndReloadTest0");
        TestAppenderToValidateLogsForCachingTests appender = new
                TestAppenderToValidateLogsForCachingTests("TestAppenderToValidateLogsForCachingTests", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStream (symbol string); " +
                "@Store(type=\"testStoreContainingInMemoryTable\", @Cache(size=\"10\", retention.period=\"1 sec\", " +
                "purge.interval=\"1 sec\"))\n" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume\n" +
                "insert into StockTable ;" +
                "@info(name = 'query2') " +
                "from DeleteStream " +
                "delete StockTable " +
                "on StockTable.symbol == symbol";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStream = siddhiAppRuntime.getInputHandler("DeleteStream");
        siddhiAppRuntime.start();

        Event[] events;
        stockStream.send(new Object[]{"WSO2", 55.6f, 1L});
        Thread.sleep(100);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        Thread.sleep(2000);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        logger.getAppenders();
        final List<String> loggedEvents = ((TestAppenderToValidateLogsForCachingTests) logger.getAppenders().
                get("TestAppenderToValidateLogsForCachingTests")).getLog();
        List<String> logMessages = new ArrayList<>();
        for (String logEvent : loggedEvents) {
            String message = String.valueOf(logEvent);
            if (message.contains(":")) {
                message = message.split(":")[1].trim();
            }
            logMessages.add(message);
        }
        Assert.assertEquals(logMessages.
                contains("store table size is smaller than max cache. Sending results from cache"), true);
        Assert.assertEquals(Collections.frequency(logMessages,
                "store table size is smaller than max cache. Sending results from cache"), 2);
        Assert.assertEquals(logMessages.contains("store table size is bigger than cache."), false);
        Assert.assertEquals(logMessages.contains("cache constraints satisfied. Checking cache"), false);
        Assert.assertEquals(logMessages.contains("cache hit. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("cache miss. Loading from store"), false);
        Assert.assertEquals(logMessages.contains("store also miss. sending null"), false);
        Assert.assertEquals(logMessages.contains("sending results from cache after loading from store"), false);
        Assert.assertEquals(logMessages.contains("sending results from store"), false);
        logger.removeAppender(appender);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void expiryAndReloadTest1() throws InterruptedException, SQLException {
        log.info("expiryAndReloadTest1");
        TestAppenderToValidateLogsForCachingTests appender = new
                TestAppenderToValidateLogsForCachingTests("TestAppenderToValidateLogsForCachingTests", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStream (symbol string); " +
                "@Store(type=\"testStoreContainingInMemoryTable\", @Cache(size=\"10\", retention.period=\"1 sec\", " +
                "purge.interval=\"1 sec\"))\n" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume\n" +
                "insert into StockTable ;" +
                "@info(name = 'query2') " +
                "from DeleteStream " +
                "delete StockTable " +
                "on StockTable.symbol == symbol";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStream = siddhiAppRuntime.getInputHandler("DeleteStream");
        siddhiAppRuntime.start();

        Event[] events;
        stockStream.send(new Object[]{"WSO2", 55.6f, 1L});
        Thread.sleep(100);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        Thread.sleep(2000);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);

        stockStream.send(new Object[]{"WSO4", 55.6f, 2L});
        stockStream.send(new Object[]{"WSO1", 55.6f, 3L});
        Thread.sleep(100);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(3, events.length);
        Thread.sleep(2000);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(3, events.length);

        stockStream.send(new Object[]{"IBM", 75.6f, 4L});
        stockStream.send(new Object[]{"WS2", 55.6f, 5L});
        Thread.sleep(2000);
        stockStream.send(new Object[]{"WSOr", 55.6f, 6L});
        Thread.sleep(100);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(6, events.length);

        final List<String> loggedEvents = ((TestAppenderToValidateLogsForCachingTests) logger.getAppenders().
                get("TestAppenderToValidateLogsForCachingTests")).getLog();
        List<String> logMessages = new ArrayList<>();
        for (String logEvent : loggedEvents) {
            String message = String.valueOf(logEvent);
            if (message.contains(":")) {
                message = message.split(":")[1].trim();
            }
            logMessages.add(message);
        }
        Assert.assertEquals(logMessages.
                contains("store table size is smaller than max cache. Sending results from cache"), true);
        Assert.assertEquals(Collections.frequency(logMessages,
                "store table size is smaller than max cache. Sending results from cache"), 5);
        Assert.assertEquals(logMessages.contains("store table size is bigger than cache."), false);
        Assert.assertEquals(logMessages.contains("cache constraints satisfied. Checking cache"), false);
        Assert.assertEquals(logMessages.contains("cache hit. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("cache miss. Loading from store"), false);
        Assert.assertEquals(logMessages.contains("store also miss. sending null"), false);
        Assert.assertEquals(logMessages.contains("sending results from cache after loading from store"), false);
        Assert.assertEquals(logMessages.contains("sending results from store"), false);
        logger.removeAppender(appender);
        siddhiAppRuntime.shutdown();
    }

    @Test // with primary key => IndexOperator
    public void expiryAndReloadTest2() throws InterruptedException, SQLException {
        log.info("expiryAndReloadTest2");
        TestAppenderToValidateLogsForCachingTests appender = new
                TestAppenderToValidateLogsForCachingTests("TestAppenderToValidateLogsForCachingTests", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStream (symbol string); " +
                "@Store(type=\"testStoreContainingInMemoryTable\", @Cache(size=\"10\", retention.period=\"1 sec\", " +
                "purge.interval=\"1 sec\"))\n" +
                "@PrimaryKey('symbol') " +
                "define table StockTable (symbol string, price float, volume long); ";

        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume\n" +
                "insert into StockTable ;" +
                "@info(name = 'query2') " +
                "from DeleteStream " +
                "delete StockTable " +
                "on StockTable.symbol == symbol";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStream = siddhiAppRuntime.getInputHandler("DeleteStream");
        siddhiAppRuntime.start();

        Event[] events;
        stockStream.send(new Object[]{"WSO2", 55.6f, 1L});
        Thread.sleep(100);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        Thread.sleep(2000);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);

        stockStream.send(new Object[]{"WSO4", 55.6f, 2L});
        stockStream.send(new Object[]{"WSO1", 55.6f, 3L});
        Thread.sleep(100);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(3, events.length);
        Thread.sleep(2000);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(3, events.length);

        stockStream.send(new Object[]{"IBM", 75.6f, 4L});
        stockStream.send(new Object[]{"WS2", 55.6f, 5L});
        Thread.sleep(2000);
        stockStream.send(new Object[]{"WSOr", 55.6f, 6L});
        Thread.sleep(100);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(6, events.length);

        final List<String> loggedEvents = ((TestAppenderToValidateLogsForCachingTests) logger.getAppenders().
                get("TestAppenderToValidateLogsForCachingTests")).getLog();
        List<String> logMessages = new ArrayList<>();
        for (String logEvent : loggedEvents) {
            String message = String.valueOf(logEvent);
            if (message.contains(":")) {
                message = message.split(":")[1].trim();
            }
            logMessages.add(message);
        }
        Assert.assertEquals(logMessages.
                contains("store table size is smaller than max cache. Sending results from cache"), true);
        Assert.assertEquals(Collections.frequency(logMessages,
                "store table size is smaller than max cache. Sending results from cache"), 5);
        Assert.assertEquals(logMessages.contains("store table size is bigger than cache."), false);
        Assert.assertEquals(logMessages.contains("cache constraints satisfied. Checking cache"), false);
        Assert.assertEquals(logMessages.contains("cache hit. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("cache miss. Loading from store"), false);
        Assert.assertEquals(logMessages.contains("store also miss. sending null"), false);
        Assert.assertEquals(logMessages.contains("sending results from cache after loading from store"), false);
        Assert.assertEquals(logMessages.contains("sending results from store"), false);
        logger.removeAppender(appender);
        siddhiAppRuntime.shutdown();
    }

    @Test // with primary key and index
    public void expiryAndReloadTest3() throws InterruptedException, SQLException {
        log.info("expiryAndReloadTest3");
        TestAppenderToValidateLogsForCachingTests appender = new
                TestAppenderToValidateLogsForCachingTests("TestAppenderToValidateLogsForCachingTests", null);
        final Logger logger = (Logger) LogManager.getRootLogger();
        logger.setLevel(Level.DEBUG);
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStream (symbol string); " +
                "@Store(type=\"testStoreContainingInMemoryTable\", @Cache(size=\"10\", retention.period=\"1 sec\", " +
                "purge.interval=\"1 sec\"))\n" +
                "@PrimaryKey('symbol') " +
                "@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query1 = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume\n" +
                "insert into StockTable ;" +
                "@info(name = 'query2') " +
                "from DeleteStream " +
                "delete StockTable " +
                "on StockTable.symbol == symbol";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query1);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStream = siddhiAppRuntime.getInputHandler("DeleteStream");
        siddhiAppRuntime.start();

        Event[] events;
        stockStream.send(new Object[]{"WSO2", 55.6f, 1L});
        Thread.sleep(100);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);
        Thread.sleep(2000);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(1, events.length);

        stockStream.send(new Object[]{"WSO4", 55.6f, 2L});
        stockStream.send(new Object[]{"WSO1", 55.6f, 3L});
        Thread.sleep(100);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(3, events.length);
        Thread.sleep(2000);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(3, events.length);

        stockStream.send(new Object[]{"IBM", 75.6f, 4L});
        stockStream.send(new Object[]{"WS2", 55.6f, 5L});
        Thread.sleep(2000);
        stockStream.send(new Object[]{"WSOr", 55.6f, 6L});
        Thread.sleep(100);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        AssertJUnit.assertEquals(6, events.length);

        final List<String> loggedEvents = ((TestAppenderToValidateLogsForCachingTests) logger.getAppenders().
                get("TestAppenderToValidateLogsForCachingTests")).getLog();
        List<String> logMessages = new ArrayList<>();
        for (String logEvent : loggedEvents) {
            String message = String.valueOf(logEvent);
            if (message.contains(":")) {
                message = message.split(":")[1].trim();
            }
            logMessages.add(message);
        }
        Assert.assertEquals(logMessages.
                contains("store table size is smaller than max cache. Sending results from cache"), true);
        Assert.assertEquals(Collections.frequency(logMessages,
                "store table size is smaller than max cache. Sending results from cache"), 5);
        Assert.assertEquals(logMessages.contains("store table size is bigger than cache."), false);
        Assert.assertEquals(logMessages.contains("cache constraints satisfied. Checking cache"), false);
        Assert.assertEquals(logMessages.contains("cache hit. Sending results from cache"), false);
        Assert.assertEquals(logMessages.contains("cache miss. Loading from store"), false);
        Assert.assertEquals(logMessages.contains("store also miss. sending null"), false);
        Assert.assertEquals(logMessages.contains("sending results from cache after loading from store"), false);
        Assert.assertEquals(logMessages.contains("sending results from store"), false);
        logger.removeAppender(appender);
        siddhiAppRuntime.shutdown();
    }
}
