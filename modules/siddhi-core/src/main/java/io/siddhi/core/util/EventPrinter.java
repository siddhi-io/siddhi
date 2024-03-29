/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import io.siddhi.core.event.Event;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Map;

/**
 * Utility class to print incoming {@link Event}
 */
public class EventPrinter {
    private static final Logger log = LogManager.getLogger(EventPrinter.class);

    public static void print(Event[] events) {
        log.info(Arrays.deepToString(events));
    }

    public static void print(Map[] events) {
        log.info(Arrays.deepToString(events));
    }

    public static void print(Map event) {
        log.info(event);
    }


    public static void print(long timestamp, Event[] inEvents, Event[] removeEvents) {
        StringBuilder sb = new StringBuilder();
        sb.append("Events{ @timestamp = ").append(timestamp).
                append(", inEvents = ").append(Arrays.deepToString(inEvents)).
                append(", RemoveEvents = ").append(Arrays.deepToString(removeEvents)).append(" }");
        log.info(sb.toString());
    }

}
