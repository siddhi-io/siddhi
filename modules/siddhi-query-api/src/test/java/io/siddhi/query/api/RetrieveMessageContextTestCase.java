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

package io.siddhi.query.api;

import io.siddhi.query.api.exception.SiddhiAppValidationException;
import io.siddhi.query.api.util.ExceptionUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RetrieveMessageContextTestCase {

    @Test
    public void testFunction1() {

        String message = ExceptionUtil.getMessageWithContext("foo", new int[]{0, 5}, new int[]{0, 10},
                "01234567890123456", "TestMessage");
        Assert.assertEquals(message, "Error on 'foo' @ Line: 0. Position: 10, " +
                "near '01234567890123456'. TestMessage");
    }

    @Test
    public void testFunction2() {

        String message = ExceptionUtil.getContext(new int[]{0, 5}, new int[]{0, 10},
                "01234567890123456");
        Assert.assertEquals(message, "56789");
    }

    @Test
    public void testFunction3() {

        Exception e = new Exception("TestMessage");
        String message = ExceptionUtil.getMessageWithContext(e, "foo",
                "01234567890123456");
        Assert.assertEquals(message, "Error on 'foo'. TestMessage");
    }

    @Test
    public void testFunction4() {

        Exception e = new SiddhiAppValidationException("TestMessage", new int[]{0, 5}, new int[]{0, 10});
        String message = ExceptionUtil.getMessageWithContext(e, "foo",
                "01234567890123456");
        Assert.assertEquals(message, "Error on 'foo' @ Line: 0. Position: 10, near '56789'. TestMessage");
    }

    @Test
    public void testFunction5() {

        Exception e = new SiddhiAppValidationException("TestMessage", new int[]{2, 5}, new int[]{4, 10});
        String message = ExceptionUtil.getMessageWithContext(e, "foo",
                "a0123456789\nb0123456789\nc0123456789\nd0123456789\n");
        Assert.assertEquals(message, "Error on 'foo' @ Line: 4. Position: 10, " +
                "near '456789\nc0123456789\nd012345678'. TestMessage");
    }
}
