/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.core.managment;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

public class ValidateTestCase {
    static final Logger log = Logger.getLogger(ValidateTestCase.class);
    private int count;
    private boolean eventArrived;
    private int inEventCount;
    private int removeEventCount;

    @Before
    public void init() {
        count = 0;
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }


    @Test
    public void ValidateTest1() throws InterruptedException {
        log.info("validate test1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "" +
                "@Plan:name('validateTest') " +
                "" +
                "define stream cseEventStream (symbol string, price float, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStream[symbol is null] " +
                "select symbol, price " +
                "insert into outputStream;";

        siddhiManager.validateExecutionPlan(executionPlan);

    }


    @Test(expected = ExecutionPlanValidationException.class)
    public void ValidateTest2() throws InterruptedException {
        log.info("validate test2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String executionPlan = "" +
                "@Plan:name('validateTest') " +
                "" +
                "define stream cseEventStream (symbol string, price float, volume long);" +
                "" +
                "@info(name = 'query1') " +
                "from cseEventStreamA[symbol is null] " +
                "select symbol, price " +
                "insert into outputStream;";


        siddhiManager.validateExecutionPlan(executionPlan);

    }
}
