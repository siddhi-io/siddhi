/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.core.executor.incremental;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.function.FunctionExecutor;
import io.siddhi.core.util.IncrementalTimeConverterUtil;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.query.api.aggregation.TimePeriod;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.exception.SiddhiAppValidationException;

/**
 * Executor class for finding the start time and end time of the within clause in incremental processing.
 * This is important when retrieving incremental aggregate values by specifying a time range with 'within' clause.
 */
public class IncrementalAggregateBaseTimeFunctionExecutor extends FunctionExecutor {
    private String timeZone;

    @Override
    protected StateFactory init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                                SiddhiQueryContext siddhiQueryContext) {
        if (attributeExpressionExecutors.length != 2) {
            throw new SiddhiAppValidationException("incrementalAggregator:getAggregationStartTime() function accepts " +
                    "two arguments, but found " + attributeExpressionExecutors.length);
        }
        if (attributeExpressionExecutors[1].getReturnType() != Attribute.Type.STRING) {
            throw new SiddhiAppValidationException("Second argument of " +
                    "incrementalAggregator:getAggregationStartTime() function accepts should be of type 'STRING', " +
                    "but found '" + attributeExpressionExecutors[1].getReturnType() + "'.");
        }
        this.timeZone = siddhiQueryContext.getSiddhiContext().getConfigManager().extractProperty(SiddhiConstants
                .AGG_TIME_ZONE);
        if (timeZone == null) {
            this.timeZone = SiddhiConstants.AGG_TIME_ZONE_DEFAULT;
        }
        return null;
    }

    @Override
    protected Object execute(Object[] data, State state) {
        long time = (long) data[0];
        String durationName = (String) data[1];
        TimePeriod.Duration duration;
        switch (durationName) {
            case "SECONDS":
                duration = TimePeriod.Duration.SECONDS;
                break;
            case "MINUTES":
                duration = TimePeriod.Duration.MINUTES;
                break;
            case "HOURS":
                duration = TimePeriod.Duration.HOURS;
                break;
            case "DAYS":
                duration = TimePeriod.Duration.DAYS;
                break;
            case "MONTHS":
                duration = TimePeriod.Duration.MONTHS;
                break;
            case "YEARS":
                duration = TimePeriod.Duration.YEARS;
                break;
            default:
                // Will not get hit
                throw new SiddhiAppRuntimeException("Duration '" + durationName + "' used for " +
                        "incrementalAggregator:aggregateBaseTime() is invalid.");
        }
        return IncrementalTimeConverterUtil.getStartTimeOfAggregates(time, duration, timeZone);
    }

    @Override
    protected Object execute(Object data, State state) {
        //More than one attribute executors
        return null;
    }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.OBJECT;
    }
}
