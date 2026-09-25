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

package io.siddhi.core.query.trigger;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class CronTriggerSchedulerTestCase {
    private static final Logger log = LogManager.getLogger(CronTriggerSchedulerTestCase.class);
    private static final String QUARTZ_WORKER_THREAD_PREFIX = "DefaultQuartzScheduler_Worker";

    private SiddhiManager siddhiManager;

    @BeforeMethod
    public void init() {
        siddhiManager = new SiddhiManager();
    }

    @AfterMethod
    public void cleanUp() {
        siddhiManager.shutdown();
    }

    @Test(priority = 1)
    public void testSameTriggerIdInTwoApps() throws InterruptedException {
        log.info("Cron triggers with the same id in two apps fire independently");
        AtomicInteger countA = new AtomicInteger();
        AtomicInteger countB = new AtomicInteger();
        SiddhiAppRuntime appA = createTriggerApp("CronAppA", countA);
        SiddhiAppRuntime appB = createTriggerApp("CronAppB", countB);
        appA.start();
        appB.start();

        Thread.sleep(2500);
        AssertJUnit.assertTrue("CronAppA did not fire", countA.get() > 0);
        AssertJUnit.assertTrue("CronAppB did not fire", countB.get() > 0);

        appA.shutdown();
        countA.set(0);
        countB.set(0);
        Thread.sleep(2500);
        AssertJUnit.assertEquals(0, countA.get());
        AssertJUnit.assertTrue("CronAppB stopped firing after CronAppA shut down", countB.get() > 0);
        appB.shutdown();
    }

    @Test(priority = 2)
    public void testSchedulerShutdownWhenIdleAndRestart() throws InterruptedException {
        log.info("Quartz scheduler is shut down when the last cron trigger stops and recreated on the next start");
        AtomicInteger count = new AtomicInteger();
        SiddhiAppRuntime app = createTriggerApp("CronAppC", count);
        app.start();
        Thread.sleep(1500);
        AssertJUnit.assertTrue(quartzWorkerThreadCount() > 0);
        app.shutdown();

        waitForQuartzWorkersToExit();
        AssertJUnit.assertEquals(0, quartzWorkerThreadCount());

        count.set(0);
        SiddhiAppRuntime redeployed = createTriggerApp("CronAppC", count);
        redeployed.start();
        Thread.sleep(2500);
        AssertJUnit.assertTrue("Redeployed CronAppC did not fire", count.get() > 0);
        redeployed.shutdown();
    }

    @Test(priority = 3)
    public void testCronWindowSurvivesTriggerShutdown() throws InterruptedException {
        log.info("Stopping the last cron trigger keeps the scheduler running for a cron window");
        String windowApp = "@app:name('CronWindowApp') " +
                "define stream InStream (symbol string); " +
                "from InStream#window.cron('*/1 * * * * ?') " +
                "select symbol " +
                "insert into OutStream;";
        SiddhiAppRuntime window = siddhiManager.createSiddhiAppRuntime(windowApp);
        AtomicInteger windowCount = new AtomicInteger();
        window.addCallback("OutStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                windowCount.addAndGet(events.length);
            }
        });
        window.start();
        SiddhiAppRuntime trigger = createTriggerApp("CronAppD", new AtomicInteger());
        trigger.start();
        Thread.sleep(1500);
        trigger.shutdown();

        InputHandler inputHandler = window.getInputHandler("InStream");
        inputHandler.send(new Object[]{"WSO2"});
        Thread.sleep(2500);
        AssertJUnit.assertTrue("Cron window stopped emitting after the cron trigger shut down",
                windowCount.get() > 0);
        window.shutdown();

        waitForQuartzWorkersToExit();
        AssertJUnit.assertEquals(0, quartzWorkerThreadCount());
    }

    private SiddhiAppRuntime createTriggerApp(String appName, AtomicInteger count) {
        String app = "@app:name('" + appName + "') " +
                "define trigger T at '*/1 * * * * ?';";
        SiddhiAppRuntime runtime = siddhiManager.createSiddhiAppRuntime(app);
        runtime.addCallback("T", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                count.addAndGet(events.length);
            }
        });
        return runtime;
    }

    private static long quartzWorkerThreadCount() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .filter(thread -> thread.getName().startsWith(QUARTZ_WORKER_THREAD_PREFIX))
                .count();
    }

    private static void waitForQuartzWorkersToExit() throws InterruptedException {
        for (int i = 0; i < 50 && quartzWorkerThreadCount() > 0; i++) {
            Thread.sleep(100);
        }
    }
}
