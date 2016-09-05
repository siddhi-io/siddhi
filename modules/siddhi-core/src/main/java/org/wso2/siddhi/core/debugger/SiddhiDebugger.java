/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.siddhi.core.debugger;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.util.snapshot.SnapshotService;

/**
 * Created on 7/20/16.
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
