package org.wso2.siddhi.core.debugger;

import java.util.Arrays;

/**
 * Created by bhagya on 7/20/16.
 */
public class SiddhiConsoleCallback implements SiddhiDebuggerCallback {
    private boolean isEventReceived = false;

    public void debuggedEvent(String queryName, Object... currentEvents) {
        System.out.println(queryName + " " + Arrays.deepToString(currentEvents));
        isEventReceived = true;
    }

    @Override
    public void printQueryState(QueryState queryStates) {
        if (queryStates != null) {
            System.out.println(queryStates.toString());
        }
        else {
            System.out.println("None");
        }
    }

    public boolean isEventReceived() {
        return isEventReceived;
    }

    public void setEventReceived(boolean eventReceived) {
        isEventReceived = eventReceived;
    }
}
