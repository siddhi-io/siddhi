/*
 * Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
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

import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.matchers.GroupMatcher;

/**
 * Coordinates access to the JVM-wide default Quartz scheduler shared by cron triggers and cron windows.
 */
public final class CronSchedulerUtil {

    /**
     * Guards scheduling and removal of jobs so that the scheduler is not shut down while a job is being added.
     */
    public static final Object LOCK = new Object();

    private CronSchedulerUtil() {
    }

    /**
     * Deletes the job and shuts the scheduler down once it has no jobs left. Must be called holding {@link #LOCK}.
     */
    public static void deleteJob(Scheduler scheduler, JobKey jobKey) throws SchedulerException {
        scheduler.deleteJob(jobKey);
        if (scheduler.getJobKeys(GroupMatcher.anyJobGroup()).isEmpty()) {
            scheduler.shutdown();
        }
    }
}
