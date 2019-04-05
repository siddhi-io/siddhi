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

package io.siddhi.core.util.parser;

import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.util.Schedulable;
import io.siddhi.core.util.Scheduler;

/**
 * This parser generates the scheduler while registering it.
 */
public class SchedulerParser {

    private SchedulerParser() {

    }

    /**
     * Create Scheduler object.
     *
     * @param singleThreadEntryValve Schedulable
     * @param siddhiQueryContext     SiddhiAppContext
     * @return Scheduler instance
     */
    public static Scheduler parse(Schedulable singleThreadEntryValve, SiddhiQueryContext siddhiQueryContext) {

        Scheduler scheduler = new Scheduler(singleThreadEntryValve, siddhiQueryContext);
        siddhiQueryContext.getSiddhiAppContext().addScheduler(scheduler);
        siddhiQueryContext.getSiddhiAppContext().addEternalReferencedHolder(scheduler);
        return scheduler;
    }
}
