package org.wso2.siddhi.core.debugger;

/**
 * Created by bhagya on 7/20/16.
 */
public interface SiddhiDebuggerCallback {
    void debuggedEvent(String queryName, Object... currentEvents);

    void printQueryState(QueryState queryStates);
}
