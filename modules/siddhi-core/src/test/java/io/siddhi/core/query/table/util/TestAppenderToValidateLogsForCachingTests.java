/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.core.query.table.util;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

import java.util.ArrayList;
import java.util.List;

@Plugin(name = "TestAppenderToValidateLogsForCachingTests",
        category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class TestAppenderToValidateLogsForCachingTests extends AbstractAppender {

    private final List<String> log = new ArrayList<>();

    public TestAppenderToValidateLogsForCachingTests(String name, Filter filter) {

        super(name, filter, null);
    }

    @PluginFactory
    public static TestAppenderToValidateLogsForCachingTests createAppender(
            @PluginAttribute("name") String name,
            @PluginElement("Filter") Filter filter) {

        return new TestAppenderToValidateLogsForCachingTests(name, filter);
    }

    @Override
    public void append(LogEvent event) {

        log.add(event.getMessage().getFormattedMessage());

    }

    public List<String> getLog() {
        List<String> clone = new ArrayList<>(log);
        log.clear();
        return clone;
    }
}
