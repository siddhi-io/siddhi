package org.wso2.siddhi.core.debugger;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.util.snapshot.SnapshotService;

/**
 * Created by bhagya on 7/20/16.
 */
public class SiddhiDebugger {
    public enum Discription {
        IN,OUT
    }
    private SiddhiBreakPoint siddhiBreakPoint;
    private SnapshotService snapshotService;
    private SiddhiDebuggerCallback siddhiDebuggerCallback;

    public SiddhiDebugger(SiddhiBreakPoint siddhiBreakPoint, ExecutionPlanContext executionPlanContext) {
        this.siddhiBreakPoint = siddhiBreakPoint;
        this.snapshotService=executionPlanContext.getSnapshotService();
    }

    public void next() {
        siddhiBreakPoint.next();
    }

    public void play() {
        siddhiBreakPoint.play();

    }

    public void setDebuggerCallback(SiddhiDebuggerCallback siddhiDebuggerCallback) {
        this.siddhiDebuggerCallback=siddhiDebuggerCallback;
        this.siddhiBreakPoint.setDebuggerCallback(siddhiDebuggerCallback);
    }

    public void acquireBreakPoint(String queryName,String breakPointType) {
        for (Discription breakpointDiscription : Discription.values()) {
            if (breakpointDiscription.name().equals(breakPointType.toUpperCase())) {
                siddhiBreakPoint.acquireBreakPoint(queryName,breakpointDiscription);
            }
        }


    }

    public void releaseBreakPoint(String queryName,String breakPointType) {
        for (Discription breakpointDiscription : Discription.values()) {
            if (breakpointDiscription.name().equals(breakPointType.toUpperCase())) {
                siddhiBreakPoint.releaseBreakPoint(queryName,breakpointDiscription);
            }
        }
    }

    public void getQueryState(String queryName) {
        QueryState queryState = snapshotService.queryState(queryName);
        siddhiDebuggerCallback.printQueryState(queryState);
    }

}
