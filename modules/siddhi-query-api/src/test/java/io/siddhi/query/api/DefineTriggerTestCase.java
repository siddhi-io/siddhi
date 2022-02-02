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

import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.definition.TriggerDefinition;
import io.siddhi.query.api.exception.DuplicateAttributeException;
import io.siddhi.query.api.exception.DuplicateDefinitionException;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.constant.TimeConstant;
import org.testng.annotations.Test;

public class DefineTriggerTestCase {

    @Test
    public void testCreatingTableDefinition() {

        SiddhiApp.siddhiApp("test").defineTrigger(TriggerDefinition.id("TriggerStream").atEvery(Expression
                .Time.day(5).value()));
    }

    @Test(expectedExceptions = DuplicateAttributeException.class)
    public void testCreatingStreamWithDuplicateAttribute() {

        TableDefinition.id("StockStream").attribute("symbol", Attribute.Type.STRING).attribute("symbol", Attribute
                .Type.INT).attribute("volume", Attribute.Type.FLOAT);
    }

    @Test
    public void testCreatingSQLTableDefinition() {

        TableDefinition.id("StockStream").attribute("symbol", Attribute.Type.STRING).attribute("price", Attribute
                .Type.INT).attribute("volume", Attribute.Type.FLOAT).annotation(Annotation.annotation("From").element
                ("datasource.id", "cepDataSource"));
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void testTriggerEventNull() {

        TriggerDefinition trigger = null;
        SiddhiApp.siddhiApp("test").defineTrigger(trigger);
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void testTriggerEventIdNull() {

        TriggerDefinition trigger = TriggerDefinition.id(null);
        SiddhiApp.siddhiApp("test").defineTrigger(trigger);
    }

    @Test
    public void testCreatingTableDefinition2() {

        SiddhiApp.siddhiApp("test").defineTrigger(TriggerDefinition.id("TriggerStream").atEvery(Expression
                .Time.week(10L).value()));
    }

    @Test
    public void testCreatingTableDefinition3() {

        SiddhiApp.siddhiApp("test").defineTrigger(TriggerDefinition.id("TriggerStream").atEvery(Expression
                .Time.week(1).value()));
    }

    @Test
    public void testTriggerDefinition() {

        TriggerDefinition triggerDefinition = new TriggerDefinition();
        triggerDefinition.setQueryContextEndIndex(new int[1]);
        triggerDefinition.setQueryContextStartIndex(new int[1]);
        SiddhiApp.siddhiApp("test").defineTrigger(TriggerDefinition.id("TriggerStream").atEvery(Expression
                .Time.day(5).value()));
    }

    @Test
    public void testTriggerDefinition2() {

        SiddhiApp app = new SiddhiApp().defineTrigger(TriggerDefinition.id("TriggerStream").atEvery(Expression
                .Time.year(1).value()));
    }

    @Test
    public void testTriggerDefinition3() {

        SiddhiApp.siddhiApp().defineTrigger(TriggerDefinition.id("TriggerStream").atEvery(Expression
                .Time.month(1).value()));
    }

    @Test(expectedExceptions = DuplicateDefinitionException.class)
    public void testTriggerDefinition4() {

        SiddhiApp.siddhiApp("test").defineTrigger(TriggerDefinition.id("TriggerStream").atEvery(Expression
                .Time.day(5).value())).defineStream(StreamDefinition.id("TriggerStream"));
    }

    @Test(expectedExceptions = DuplicateDefinitionException.class)
    public void testTriggerDefinition5() {

        SiddhiApp.siddhiApp("test").defineTrigger(TriggerDefinition.id("TriggerStream").atEvery(Expression
                .Time.day(5).value())).defineTrigger(TriggerDefinition.id("TriggerStream"));
    }

    @Test
    public void testTriggerDefinition6() {

        SiddhiApp.siddhiApp("test").defineTrigger(TriggerDefinition.id("TriggerStream").
                atEvery(new TimeConstant(1000)).at("start"));
    }
}
