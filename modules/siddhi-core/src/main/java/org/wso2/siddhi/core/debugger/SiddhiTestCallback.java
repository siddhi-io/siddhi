package org.wso2.siddhi.core.debugger;

/**
 * Created by bhagya on 7/20/16.
 */
public class SiddhiTestCallback implements SiddhiDebuggerCallback {
    private boolean isEventReceived = false;
    private Object[] currentEvents;
    private QueryState queryStates;
    @Override
    public void debuggedEvent(String queryName, Object... currentEvents) {
        this.currentEvents=currentEvents;
        isEventReceived=true;
    }

    @Override
    public void printQueryState(QueryState queryStates) {
        this.queryStates=queryStates;

    }

    public Object[] getCurrentEvents() {
        return currentEvents;
    }

    public QueryState getQueryStates() {
        if(queryStates!=null) {
            return queryStates;
        }else {
            return null;
        }
    }
    public boolean isEventReceived() {
        return isEventReceived;
    }

    public void setEventReceived(boolean eventReceived) {
        isEventReceived = eventReceived;
    }
}
