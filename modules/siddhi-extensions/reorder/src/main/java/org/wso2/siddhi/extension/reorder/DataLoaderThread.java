package org.wso2.siddhi.extension.reorder;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
/**
 * Created by vithursa on 9/11/16.
 */
public interface DataLoaderThread extends Runnable {
    public void run();
    public LinkedBlockingQueue getEventBuffer();
}

