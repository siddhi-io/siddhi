package org.wso2.siddhi.core.debugger;

import java.util.Scanner;

/**
 * Created by bhagya on 7/20/16.
 */
public class DebuggerCommandLineClient implements Runnable {
    final String play = "play";
    final String next = "next";
    final String start = "start";
    final String stop = "stop";
    final String acquireBreakPoint = "bp acquire ";
    final String releaseBreakPoint = "bp release ";
    final String printState = "state ";
    private final SiddhiConsoleCallback siddhiDebuggerCallback;
    SiddhiDebugger siddhiDebugger;
    Scanner input = new Scanner(System.in);
    boolean started = false;

    public DebuggerCommandLineClient(SiddhiDebugger siddhiDebugger) {
        this.siddhiDebugger = siddhiDebugger;
        siddhiDebuggerCallback = new SiddhiConsoleCallback();
        siddhiDebugger.setDebuggerCallback(siddhiDebuggerCallback);
    }

    @Override
    public void run() {
        waitForEvents();
        while (true) {
            System.out.println("Enter action to be performed: ");
            String userInput = input.nextLine();
            if (started) {
                if (userInput.equalsIgnoreCase(stop)) {
                    started = false;
                    System.exit(0);
                    break;
                } else if (userInput.startsWith(acquireBreakPoint.toLowerCase())) {
                    String[] elements=userInput.split(" ");
                    siddhiDebugger.acquireBreakPoint(elements[2],elements[3]);
                } else if (userInput.startsWith(releaseBreakPoint.toLowerCase())) {
                    String[] elements=userInput.split(" ");
                    siddhiDebugger.releaseBreakPoint(elements[2],elements[3]);
                } else if (userInput.startsWith(printState.toLowerCase())) {
                    siddhiDebugger.getQueryState(userInput.substring(printState.length()).trim());
                } else if (userInput.equalsIgnoreCase(next)) {
                    siddhiDebugger.next();
                    waitForEvents();

                } else if (userInput.equalsIgnoreCase(play)) {
                    siddhiDebugger.play();
                    waitForEvents();

                } else {
                    System.out.println("'" + userInput + "' not supported, expecting " + stop + ", " + next + ", " + play + ", " + printState + ", " + acquireBreakPoint + " <bp id>, " + releaseBreakPoint + " <bp id>");
                }
            } else {
                System.out.println("start already called!");
            }

        }

    }

    private void waitForEvents() {
        while (!siddhiDebuggerCallback.isEventReceived()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        siddhiDebuggerCallback.setEventReceived(false);
    }

    public void startDebugging() {
        while (true) {
            System.out.println("Enter action to be performed: ");
            String userInput = input.nextLine();
            if (!started) {
                if (userInput.equalsIgnoreCase(start)) {
                    started = true;
                    Thread thread = new Thread(this);
                    thread.start();
                    break;
                } else if (userInput.startsWith(acquireBreakPoint.toLowerCase())) {
                    String[] elements=userInput.split(" ");
                    siddhiDebugger.acquireBreakPoint(elements[2],elements[3]);
                } else if (userInput.startsWith(releaseBreakPoint.toLowerCase())) {
                    String[] elements=userInput.split(" ");
                    siddhiDebugger.releaseBreakPoint(elements[2],elements[3]);
                } else {
                    System.out.println("'" + userInput + "' not supported, expecting " + start + ", " + acquireBreakPoint + " <bp id>, " + releaseBreakPoint + " <bp id>");
                }
            } else {
                System.out.println("start already called!");
            }
        }
    }

}
