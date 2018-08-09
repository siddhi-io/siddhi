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

package org.wso2.siddhi.core.executor.incremental;

import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.core.util.IncrementalTimeConverterUtil;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.aggregation.TimePeriod;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.Map;

/**
 * Executor class for finding the start time and end time of the within clause in incremental processing.
 * This is important when retrieving incremental aggregate values by specifying a time range with 'within' clause.
 */
public class IncrementalAggregateBaseTimeFunctionExecutor extends FunctionExecutor {

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ConfigReader configReader,
                        SiddhiAppContext siddhiAppContext) {
        if (attributeExpressionExecutors.length != 2) {
            throw new SiddhiAppValidationException("incrementalAggregator:getAggregationStartTime() function accepts " +
                    "two arguments, but found " + attributeExpressionExecutors.length);
        }
        if (attributeExpressionExecutors[1].getReturnType() != Attribute.Type.STRING) {
            throw new SiddhiAppValidationException("Second argument of " +
                    "incrementalAggregator:getAggregationStartTime() function accepts should be of type 'STRING', " +
                    "but found '" + attributeExpressionExecutors[1].getReturnType() + "'.");
        }
    }

    @Override
    protected Object execute(Object[] data) {
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
        return IncrementalTimeConverterUtil.getStartTimeOfAggregates(time, duration);
    }

    @Override
    protected Object execute(Object data) {
        //More than one attribute executors
        return null;
    }

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.OBJECT;
    }

    @Override
    public Map<String, Object> currentState() {
        return null; // No states
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        // Nothing to be done
    }
}
