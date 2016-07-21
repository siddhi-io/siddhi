package org.wso2.siddhi.core.debugger;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

/**
 * Created by bhagya on 7/20/16.
 */
public class Client {
    static final Logger log = Logger.getLogger(Client.class);
    public static String userInput;
    private static volatile int count;
    private volatile boolean eventArrived;

    public static void main(String[] args) {
        log.info("User class");

        SiddhiManager siddhiManager = new SiddhiManager();

        String cseEventStream = "@config(async = 'true') define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1')" +
                "from cseEventStream#window.length(5) " +
                "select symbol,price,volume insert into largerThanFiftyStream;" +
                "@info(name = 'query2') from largerThanFiftyStream#window.length(5)[volume > 100l] select symbol,price,volume insert into largerThanHundredStream";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        executionPlanRuntime.addCallback("largerThanHundredStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                count = count + events.length;
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        SiddhiDebugger siddhiDebugger = executionPlanRuntime.debug();
        DebuggerCommandLineClient debuggerCommandLineClient = new DebuggerCommandLineClient(siddhiDebugger);
        debuggerCommandLineClient.startDebugging();
        try {
            Thread.sleep(1000);
            inputHandler.send(new Object[]{"WSO2", 50f, 60});
            Thread.sleep(1000);
            inputHandler.send(new Object[]{"WSO2", 60f, 160});
            Thread.sleep(1000);
            inputHandler.send(new Object[]{"WSO2", 450f, 200});
            Thread.sleep(1000);
            inputHandler.send(new Object[]{"WSO2", 72f, 10});
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted Exception", e);
        }

        executionPlanRuntime.shutdown();
        System.exit(0);
    }

    public void init() {
        count = 0;
        eventArrived = false;
    }
}
