/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.core.util;

import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.query.api.aggregation.TimePeriod;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Performs time conversions related to incremental aggregation.
 */
public class IncrementalTimeConverterUtil {

    public static long getNextEmitTime(long currentTime, TimePeriod.Duration duration, String timeZone) {
        switch (duration) {
            case SECONDS:
                return currentTime - currentTime % 1000 + 1000;
            case MINUTES:
                return currentTime - currentTime % 60000 + 60000;
            case HOURS:
                return getNextEmitTimeForHour(currentTime, timeZone);
            case DAYS:
                return getNextEmitTimeForDay(currentTime, timeZone);
            case MONTHS:
                return getNextEmitTimeForMonth(currentTime, timeZone);
            case YEARS:
                return getNextEmitTimeForYear(currentTime, timeZone);
            default:
                throw new SiddhiAppRuntimeException("Undefined duration " + duration.toString());
        }
    }

    public static long getStartTimeOfAggregates(long currentTime, TimePeriod.Duration duration, String timeZone) {
        switch (duration) {
            case SECONDS:
                return currentTime - currentTime % getMillisecondsPerDuration(duration);
            case MINUTES:
                return currentTime - currentTime % getMillisecondsPerDuration(duration);
            case HOURS:
                return getStartTimeOfAggregatesForHour(currentTime, timeZone);
            case DAYS:
                return getStartTimeOfAggregatesForDay(currentTime, timeZone);
            case MONTHS:
                return getStartTimeOfAggregatesForMonth(currentTime, timeZone);
            case YEARS:
                return getStartTimeOfAggregatesForYear(currentTime, timeZone);
            default:
                throw new SiddhiAppRuntimeException("Undefined duration " + duration.toString());
        }
    }

    public static long getPreviousStartTime(long currentStartTime, TimePeriod.Duration duration) {
        switch (duration) {
            case SECONDS:
                return currentStartTime - getMillisecondsPerDuration(duration);
            case MINUTES:
                return currentStartTime - getMillisecondsPerDuration(duration);
            case HOURS:
                return currentStartTime - getMillisecondsPerDuration(duration);
            case DAYS:
                return currentStartTime - getMillisecondsPerDuration(duration);
            case MONTHS:
                return getStartTimeOfPreviousMonth(currentStartTime);
            case YEARS:
                return getStartTimeOfPreviousYear(currentStartTime);
            default:
                throw new SiddhiAppRuntimeException("Undefined duration " + duration.toString());
        }
    }

    private static long getNextEmitTimeForHour(long currentTime, String timeZone) {
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(currentTime),
                ZoneId.of(timeZone));
        if (zonedDateTime.getHour() == 23) {
            if (zonedDateTime.getDayOfMonth() + 1 > zonedDateTime.getMonth().length(zonedDateTime.getYear() % 4 == 0)) {
                // if the current day is the last day of the month
                if (zonedDateTime.getMonthValue() == 12) {
                    // For a time in December, emit time should be beginning of January next year
                    return ZonedDateTime
                            .of(zonedDateTime.getYear() + 1, 1, 1, 0, 0, 0, 0, ZoneId.of(timeZone))
                            .toEpochSecond() * 1000;
                } else {
                    // For any other month, the 1st day of next month must be considered
                    return ZonedDateTime.of(zonedDateTime.getYear(), zonedDateTime.getMonthValue() + 1, 1, 0, 0, 0, 0,
                            ZoneId.of(timeZone)).toEpochSecond() * 1000;
                }
            } else {
                // for any other days
                return ZonedDateTime
                        .of(zonedDateTime.getYear(), zonedDateTime.getMonthValue(), zonedDateTime.getDayOfMonth() + 1,
                                0, 0, 0, 0, ZoneId.of(timeZone)).toEpochSecond() * 1000;
            }
        } else {
            return ZonedDateTime
                    .of(zonedDateTime.getYear(), zonedDateTime.getMonthValue(), zonedDateTime.getDayOfMonth(),
                            zonedDateTime.getHour() + 1, 0, 0, 0, ZoneId.of(timeZone)).toEpochSecond() * 1000;
        }
    }

    private static long getNextEmitTimeForDay(long currentTime, String timeZone) {
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(currentTime),
                ZoneId.of(timeZone));
        if (zonedDateTime.getDayOfMonth() + 1 > zonedDateTime.getMonth().length(zonedDateTime.getYear() % 4 == 0)) {
            // if the current day is the last day of the month
            if (zonedDateTime.getMonthValue() == 12) {
                // For a time in December, emit time should be beginning of January next year
                return ZonedDateTime
                        .of(zonedDateTime.getYear() + 1, 1, 1, 0, 0, 0, 0, ZoneId.of(timeZone))
                        .toEpochSecond() * 1000;
            } else {
                // For any other month, the 1st day of next month must be considered
                return ZonedDateTime.of(zonedDateTime.getYear(), zonedDateTime.getMonthValue() + 1, 1, 0, 0, 0, 0,
                        ZoneId.of(timeZone)).toEpochSecond() * 1000;
            }
        } else {
            // for any other days
            return ZonedDateTime
                    .of(zonedDateTime.getYear(), zonedDateTime.getMonthValue(), zonedDateTime.getDayOfMonth() + 1, 0,
                            0, 0, 0, ZoneId.of(timeZone)).toEpochSecond() * 1000;
        }
    }

    private static long getNextEmitTimeForMonth(long currentTime, String timeZone) {
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(currentTime),
                ZoneId.of(timeZone));
        if (zonedDateTime.getMonthValue() == 12) {
            // For a time in December, emit time should be beginning of January next year
            return ZonedDateTime
                    .of(zonedDateTime.getYear() + 1, 1, 1, 0, 0, 0, 0, ZoneId.of(timeZone))
                    .toEpochSecond() * 1000;
        } else {
            // For any other month, the 1st day of next month must be considered
            return ZonedDateTime.of(zonedDateTime.getYear(), zonedDateTime.getMonthValue() + 1, 1, 0, 0, 0, 0,
                    ZoneId.of(timeZone)).toEpochSecond() * 1000;
        }
    }

    private static long getNextEmitTimeForYear(long currentTime, String timeZone) {
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(currentTime), ZoneId.of(timeZone));
        return ZonedDateTime
                .of(zonedDateTime.getYear() + 1, 1, 1, 0, 0, 0, 0, ZoneId.of(timeZone))
                .toEpochSecond() * 1000;
    }

    private static long getStartTimeOfAggregatesForHour(long currentTime, String timeZone) {
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(currentTime),
                ZoneId.of(timeZone));
        return ZonedDateTime.of(zonedDateTime.getYear(), zonedDateTime.getMonthValue(), zonedDateTime.getDayOfMonth(),
                zonedDateTime.getHour(), 0, 0, 0, ZoneId.of(timeZone)).toEpochSecond() * 1000;
    }

    private static long getStartTimeOfAggregatesForDay(long currentTime, String timeZone) {
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(currentTime),
                ZoneId.of(timeZone));
        return ZonedDateTime.of(zonedDateTime.getYear(), zonedDateTime.getMonthValue(), zonedDateTime.getDayOfMonth(),
                0, 0, 0, 0, ZoneId.of(timeZone)).toEpochSecond() * 1000;
    }

    private static long getStartTimeOfAggregatesForMonth(long currentTime, String timeZone) {
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(currentTime),
                ZoneId.of(timeZone));
        return ZonedDateTime.of(zonedDateTime.getYear(), zonedDateTime.getMonthValue(), 1, 0, 0, 0, 0,
                ZoneId.of(timeZone)).toEpochSecond() * 1000;
    }

    private static long getStartTimeOfAggregatesForYear(long currentTime, String timeZone) {
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(currentTime),
                ZoneId.of(timeZone));
        return ZonedDateTime
                .of(zonedDateTime.getYear(), 1, 1, 0, 0, 0, 0, ZoneId.of(timeZone))
                .toEpochSecond() * 1000;
    }

    private static long getStartTimeOfPreviousMonth(long currentEmitTime) {
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(currentEmitTime),
                ZoneId.of("GMT"));
        int givenMonth = zonedDateTime.getMonthValue();
        int givenYear = zonedDateTime.getYear();

        if (givenMonth == 1) {
            return ZonedDateTime.of(--givenYear, 12, 1, 0, 0, 0, 0, ZoneId.of("GMT"))
                    .toEpochSecond() * 1000;
        } else {
            return ZonedDateTime
                    .of(givenYear, --givenMonth, 1, 0, 0, 0, 0, ZoneId.of("GMT"))
                    .toEpochSecond() * 1000;
        }
    }

    private static long getStartTimeOfPreviousYear(long currentEmitTime) {
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(currentEmitTime),
                ZoneId.of("GMT"));
        int givenYear = zonedDateTime.getYear();
        return ZonedDateTime.of(--givenYear, 1, 1, 0, 0, 0, 0, ZoneId.of("GMT"))
                .toEpochSecond() * 1000;
    }

    public static int getMillisecondsPerDuration(TimePeriod.Duration duration) {
        switch (duration) {
            case SECONDS:
                return 1000;
            case MINUTES:
                return 60000;
            case HOURS:
                return 3600000;
            case DAYS:
                return 86400000;
            default:
                throw new SiddhiAppRuntimeException("Cannot provide number of milliseconds per duration " + duration
                        + ".Number of milliseconds are only define for SECONDS, MINUTES, HOURS and DAYS");
        }
    }

    public static boolean isAggregationDataComplete(long timestamp, TimePeriod.Duration duration, String timeZone) {
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp),
                ZoneId.of(timeZone));
        ZonedDateTime zonedCurrentDateTime = ZonedDateTime.ofInstant(Instant.now(), ZoneId.of(timeZone));
        switch (duration) {
            case SECONDS:
                return false;
            case MINUTES:
                return zonedDateTime.getYear() == zonedCurrentDateTime.getYear() &&
                        zonedDateTime.getMonthValue() == zonedCurrentDateTime.getDayOfMonth() &&
                        zonedDateTime.getDayOfMonth() == zonedCurrentDateTime.getDayOfMonth() &&
                        zonedDateTime.getHour() == zonedCurrentDateTime.getHour() &&
                        zonedDateTime.getMinute() == (zonedCurrentDateTime.getMinute() - 1);
            case HOURS:
                return zonedDateTime.getYear() == zonedCurrentDateTime.getYear() &&
                        zonedDateTime.getMonthValue() == zonedCurrentDateTime.getDayOfMonth() &&
                        zonedDateTime.getDayOfMonth() == zonedCurrentDateTime.getDayOfMonth() &&
                        zonedDateTime.getHour() == (zonedCurrentDateTime.getHour() - 1);
            case DAYS:
                return zonedDateTime.getYear() == zonedCurrentDateTime.getYear() &&
                        zonedDateTime.getMonthValue() == zonedCurrentDateTime.getDayOfMonth() &&
                        zonedDateTime.getDayOfMonth() == (zonedCurrentDateTime.getDayOfMonth() - 1);
            case MONTHS:
                return zonedDateTime.getYear() == zonedCurrentDateTime.getYear() &&
                        zonedDateTime.getMonthValue() == (zonedCurrentDateTime.getMonthValue() - 1);
            case YEARS:
                return zonedDateTime.getYear() == (zonedCurrentDateTime.getYear() - 1);
        }
        return false;
    }
}
