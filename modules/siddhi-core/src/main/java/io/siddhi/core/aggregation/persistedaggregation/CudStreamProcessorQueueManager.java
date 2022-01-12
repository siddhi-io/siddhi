/*
 * Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.core.aggregation.persistedaggregation;

import io.siddhi.core.event.ComplexEventChunk;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Implements functions to manage the queue
 **/
public class CudStreamProcessorQueueManager implements Runnable {
    private static final Logger log = LogManager.getLogger(CudStreamProcessorQueueManager.class);
    private volatile boolean run = true;
    private LinkedBlockingQueue<QueuedCudStreamProcessor> cudStreamProcessorQueue;

    public LinkedBlockingQueue<QueuedCudStreamProcessor> initializeAndGetCudStreamProcessorQueue() {
        cudStreamProcessorQueue = new LinkedBlockingQueue<QueuedCudStreamProcessor>();
        return cudStreamProcessorQueue;
    }

    public LinkedBlockingQueue<QueuedCudStreamProcessor> getCudStreamProcessorQueue() {
        return this.cudStreamProcessorQueue;
    }

    @Override
    public void run() {
        while (run) {
            QueuedCudStreamProcessor queuedCudStreamProcessor = null;
            try {
                queuedCudStreamProcessor = this.cudStreamProcessorQueue.take();
            } catch (InterruptedException e) {
                log.warn("Thread interrupted. Error when trying to retrieve queued values." + e.getMessage());
            }
            if (null != queuedCudStreamProcessor) {
                if (log.isDebugEnabled()) {
                    log.debug("Current queue size is = " + cudStreamProcessorQueue.size());
                }
                int i = 0;
                while (true) {
                    i++;
                    try {
                        ComplexEventChunk complexEventChunk = new ComplexEventChunk();
                        complexEventChunk.add(queuedCudStreamProcessor.getStreamEvent());
                        if (log.isDebugEnabled()) {
                            log.debug("Starting processing for duration " + queuedCudStreamProcessor.getDuration());
                        }
                        queuedCudStreamProcessor.getCudStreamProcessor().
                                process(complexEventChunk);
                        complexEventChunk.clear();
                        if (log.isDebugEnabled()) {
                            log.debug("End processing for duration " + queuedCudStreamProcessor.getDuration());
                        }
                        break;
                    } catch (Exception e) {
                        if (e.getCause() instanceof SQLException) {
                            if (e.getCause().getLocalizedMessage().contains("try restarting transaction") && i < 3) {
                                log.error("Error occurred while executing the aggregation for data between " +
                                        queuedCudStreamProcessor.getStartTimeOfNewAggregates() + " - " +
                                        queuedCudStreamProcessor.getEmittedTime() + " for duration " +
                                        queuedCudStreamProcessor.getDuration() +
                                        " Retrying the transaction attempt " + (i - 1), e);
                                try {
                                    Thread.sleep(3000);
                                } catch (InterruptedException interruptedException) {
                                    log.error("Thread sleep interrupted while waiting to re-execute the " +
                                            "aggregation query for duration " +
                                            queuedCudStreamProcessor.getDuration(), interruptedException);
                                }
                                continue;
                            }
                            log.error("Error occurred while executing the aggregation for data between "
                                    + queuedCudStreamProcessor.getStartTimeOfNewAggregates() + " - " +
                                    queuedCudStreamProcessor.getEmittedTime() + " for duration " +
                                    queuedCudStreamProcessor.getDuration() +
                                    ". Attempted re-executing the query for 9 seconds. " +
                                    "This Should be investigated since this will lead to a data mismatch\n", e);
                        } else {
                            log.error("Error occurred while executing the aggregation for data between "
                                    + queuedCudStreamProcessor.getStartTimeOfNewAggregates() + " - " +
                                    queuedCudStreamProcessor.getEmittedTime() + " for duration \n" +
                                    queuedCudStreamProcessor.getDuration(), e);
                        }
                        break;
                    }
                }
            }
        }
    }

}


