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
package org.wso2.siddhi.core.debugger;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;

public class TestDebuggerClient {

    private static final Logger log = Logger.getLogger(TestDebuggerClient.class);
    private final PrintStream originalOut = System.out;
    private final InputStream originalIn = System.in;
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();

    @Before
    public void setUpStreams() {
        System.setOut(new PrintStream(outContent));
    }

    @After
    public void cleanUpStreams() {
        System.setOut(originalOut);
        System.setIn(originalIn);
    }

    @Test
    public void testDebugger1() throws InterruptedException {
        log.info("Siddi Debugger Test 1: Test next traversal in a simple query");

        String executionPlanPath = getClass().getClassLoader().getResource("debugger_executionplan.siddhiql").getPath();
        String inputPath = getClass().getClassLoader().getResource("debugger_input.txt").getPath();

        ByteArrayInputStream in = new ByteArrayInputStream(("add breakpoint " +
                "query1:in\nstart\nnext\nnext\nnext\nnext\nstop\n").getBytes());
        System.setIn(in);

        SiddhiDebuggerClient.main(new String[]{executionPlanPath, inputPath});

        Thread.sleep(100);

        String output = outContent.toString();
        log.info(output);

        Assert.assertEquals("Incorrect number of debug points", 4, countMatches(output, "@Debug"));
    }

    /**
     * Count the number of occurrences of a substring in a text.
     *
     * @param text  the complete text
     * @param token the substring
     * @return number of occurrences
     */
    private int countMatches(String text, String token) {
        int lastIndex = 0;
        int count = 0;

        while (lastIndex != -1) {
            lastIndex = text.indexOf(token, lastIndex);
            if (lastIndex != -1) {
                count++;
                lastIndex += token.length();
            }
        }
        return count;
    }
}
