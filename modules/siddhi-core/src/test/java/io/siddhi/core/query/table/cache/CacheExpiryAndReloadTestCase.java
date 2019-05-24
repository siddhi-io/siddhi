package io.siddhi.core.query.table.cache;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.table.util.TestAppender;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class CacheExpiryAndReloadTestCase {
    private static final Logger log = Logger.getLogger(CacheExpiryAndReloadTestCase.class);
    @Test
    public void insertIntoTableWithCacheTest5() throws InterruptedException, SQLException {
        //Configure siddhi to insert events data to table only from specific fields of the stream.
        log.info("insertIntoTableWithCacheTest1");

        final TestAppender appender = new TestAppender();
        final Logger logger = Logger.getRootLogger();
        logger.addAppender(appender);
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStream (symbol string); " +
                "@Store(type=\"testStoreContainingInMemoryTable\", @Cache(size=\"10\", expiry.time=\"1 sec\", " +
                "expiry.check.interval=\"1 sec\"))\n" +
                //"@Index(\"volume\")" +
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
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        Thread.sleep(2000);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);

        stockStream.send(new Object[]{"WSO4", 55.6f, 2L});
        stockStream.send(new Object[]{"WSO1", 55.6f, 3L});
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
        Thread.sleep(2000);
        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);

        stockStream.send(new Object[]{"IBM", 75.6f, 4L});
        stockStream.send(new Object[]{"WS2", 55.6f, 5L});
        Thread.sleep(2000);
        stockStream.send(new Object[]{"WSOr", 55.6f, 6L});

        events = siddhiAppRuntime.query("" +
                "from StockTable ");
        EventPrinter.print(events);
//        AssertJUnit.assertEquals(2, events.length);

        final List<LoggingEvent> log = appender.getLog();
        List<String> logMessages = new ArrayList<>();
        for (LoggingEvent logEvent : log) {
            logMessages.add(String.valueOf(logEvent.getMessage()));
        }

        siddhiAppRuntime.shutdown();
    }
}
