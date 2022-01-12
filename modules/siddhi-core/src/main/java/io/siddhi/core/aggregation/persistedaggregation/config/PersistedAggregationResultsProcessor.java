/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.core.aggregation.persistedaggregation.config;

import io.siddhi.core.event.ComplexEvent;
import io.siddhi.core.event.ComplexEventChunk;
import io.siddhi.core.query.processor.Processor;
import io.siddhi.query.api.aggregation.TimePeriod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Date;
import java.util.List;

/**
 * Processor implementation to read the results after executing the aggregation query
 **/
public class PersistedAggregationResultsProcessor implements Processor {
    private static final Logger log = LogManager.getLogger(PersistedAggregationResultsProcessor.class);
    private TimePeriod.Duration duration;

    public PersistedAggregationResultsProcessor(TimePeriod.Duration duration) {
        this.duration = duration;
    }

    @Override
    public void process(ComplexEventChunk complexEventChunk) {
        if (complexEventChunk != null) {
            ComplexEvent complexEvent = complexEventChunk.getFirst();
            Object[] outputData = complexEvent.getOutputData();
            if (outputData.length == 3) {
                Date fromTime = new Date((Long) outputData[0]);
                Date toTime = new Date((Long) outputData[1]);
                log.debug("Aggregation executed for duration " + duration + " from " + fromTime + " to " +
                        toTime + " and  " + outputData[2] + " records has been successfully updated ");
            }
        }
    }

    @Override
    public void process(List<ComplexEventChunk> complexEventChunks) {

    }

    @Override
    public Processor getNextProcessor() {
        return null;
    }

    @Override
    public void setNextProcessor(Processor processor) {

    }

    @Override
    public void setToLast(Processor processor) {

    }
}
