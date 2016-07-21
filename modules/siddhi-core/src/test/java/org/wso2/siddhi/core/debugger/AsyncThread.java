package org.wso2.siddhi.core.debugger;

import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.List;

/**
 * Created by bhagya on 7/21/16.
 */
public class AsyncThread implements Runnable {
    InputHandler inputHandler;
    List<Object[]> eventsToSend;

    public AsyncThread(InputHandler inputHandler, List<Object[]> eventsToSend) {
        this.inputHandler = inputHandler;
        this.eventsToSend = eventsToSend;
    }

    @Override
    public void run() {

        try {
            for (Object[] events:eventsToSend) {
                inputHandler.send(events);
                Thread.sleep(1000);
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
