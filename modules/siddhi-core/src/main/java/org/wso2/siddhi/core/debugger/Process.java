package org.wso2.siddhi.core.debugger;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by bhagya on 7/20/16.
 */
public class Process {
    private static final AtomicBoolean queryCalled = new AtomicBoolean(false);

    public static final ThreadLocal<AtomicBoolean> threadId =
            new ThreadLocal<AtomicBoolean>() {
                @Override
                protected AtomicBoolean initialValue() {
                    return queryCalled;
                }
            };

    public static AtomicBoolean get() {
        return threadId.get();
    }
}
