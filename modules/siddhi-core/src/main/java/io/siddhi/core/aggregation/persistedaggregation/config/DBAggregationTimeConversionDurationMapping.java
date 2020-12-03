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

import io.siddhi.query.api.aggregation.TimePeriod;

/**
 * This class represents the duration objects that needed to pass in to time conversion function for each database type
 * inorder to normalize the AGG_EVENT_TIMESTAMP
 **/
public class DBAggregationTimeConversionDurationMapping {
    private String day;
    private String month;
    private String year;

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getDurationMapping(TimePeriod.Duration duration) {
        switch (duration) {
            case DAYS:
                return day;
            case MONTHS:
                return month;
            case YEARS:
                return year;
            default:
                //this won't get hit since persisted aggregators only will be created days and upward.
                return "";
        }
    }
}
