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
package io.siddhi.core.stream.event;

import io.siddhi.core.aggregation.AggregationRuntime;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.config.SiddhiContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.event.state.MetaStateEvent;
import io.siddhi.core.event.stream.MetaStreamEvent;
import io.siddhi.core.event.stream.StreamEvent;
import io.siddhi.core.event.stream.StreamEventFactory;
import io.siddhi.core.event.stream.converter.SelectiveStreamEventConverter;
import io.siddhi.core.event.stream.converter.SimpleStreamEventConverter;
import io.siddhi.core.event.stream.converter.StreamEventConverter;
import io.siddhi.core.event.stream.converter.StreamEventConverterFactory;
import io.siddhi.core.event.stream.converter.ZeroStreamEventConverter;
import io.siddhi.core.exception.OperationNotSupportedException;
import io.siddhi.core.executor.ConstantExpressionExecutor;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.executor.VariableExpressionExecutor;
import io.siddhi.core.executor.condition.AndConditionExpressionExecutor;
import io.siddhi.core.executor.condition.compare.greaterthan.GreaterThanCompareConditionExpressionExecutorIntInt;
import io.siddhi.core.executor.condition.compare.lessthan.LessThanCompareConditionExpressionExecutorFloatFloat;
import io.siddhi.core.executor.math.add.AddExpressionExecutorFloat;
import io.siddhi.core.query.QueryRuntimeImpl;
import io.siddhi.core.query.input.stream.single.SingleStreamRuntime;
import io.siddhi.core.stream.input.source.Source;
import io.siddhi.core.stream.output.sink.Sink;
import io.siddhi.core.table.Table;
import io.siddhi.core.util.IdGenerator;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.event.handler.EventExchangeHolderFactory;
import io.siddhi.core.util.lock.LockSynchronizer;
import io.siddhi.core.util.parser.QueryParser;
import io.siddhi.core.util.parser.helper.QueryParserHelper;
import io.siddhi.core.util.snapshot.SnapshotService;
import io.siddhi.core.window.Window;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.stream.InputStream;
import io.siddhi.query.api.execution.query.selection.Selector;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.condition.Compare;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventTestCase {

    @Test
    public void testEventCreation() {
        EventExchangeHolderFactory eventExchangeHolderFactory = new EventExchangeHolderFactory(2);
        AssertJUnit.assertEquals(2, eventExchangeHolderFactory.newInstance().getEvent().getData().length);

        StreamEventFactory streamEventFactory = new StreamEventFactory(2, 3, 4);
        StreamEvent streamEvent = streamEventFactory.newInstance();
        AssertJUnit.assertEquals(2, streamEvent.getBeforeWindowData().length);
        AssertJUnit.assertEquals(3, streamEvent.getOnAfterWindowData().length);
        AssertJUnit.assertEquals(4, streamEvent.getOutputData().length);
    }

    @Test
    public void testPassThroughStreamEventConverter() {
        Attribute symbol = new Attribute("symbol", Attribute.Type.STRING);
        Attribute price = new Attribute("price", Attribute.Type.DOUBLE);
        Attribute volume = new Attribute("volume", Attribute.Type.INT);

        MetaStreamEvent metaStreamEvent = new MetaStreamEvent();
        metaStreamEvent.addOutputDataAllowingDuplicate(symbol);
        metaStreamEvent.addOutputDataAllowingDuplicate(price);
        metaStreamEvent.addOutputDataAllowingDuplicate(volume);


        StreamDefinition streamDefinition = StreamDefinition.id("cseEventStream").attribute("symbol", Attribute.Type
                .STRING).attribute("price", Attribute.Type.DOUBLE).attribute("volume", Attribute.Type.INT);
        Event event = new Event(System.currentTimeMillis(), new Object[]{"WSO2", 200.0, 50});

        metaStreamEvent.addInputDefinition(streamDefinition);

        StreamEventConverter converter = StreamEventConverterFactory.constructEventConverter(metaStreamEvent);
        StreamEventFactory eventPool = new StreamEventFactory(metaStreamEvent);

        StreamEvent newEvent = eventPool.newInstance();
        converter.convertEvent(event, newEvent);

        AssertJUnit.assertTrue(converter instanceof ZeroStreamEventConverter);
        AssertJUnit.assertEquals(3, newEvent.getOutputData().length);

        AssertJUnit.assertEquals("WSO2", newEvent.getOutputData()[0]);
        AssertJUnit.assertEquals(200.0, newEvent.getOutputData()[1]);
        AssertJUnit.assertEquals(50, newEvent.getOutputData()[2]);
    }

    @Test
    public void testSimpleStreamEventConverter() {
        Attribute price = new Attribute("price", Attribute.Type.DOUBLE);
        Attribute symbol = new Attribute("symbol", Attribute.Type.STRING);

        MetaStreamEvent metaStreamEvent = new MetaStreamEvent();
        metaStreamEvent.addOutputData(symbol);
        metaStreamEvent.addOutputData(price);

        StreamDefinition streamDefinition = StreamDefinition.id("cseEventStream").attribute("symbol", Attribute.Type
                .STRING).attribute("price", Attribute.Type.DOUBLE).attribute("volume", Attribute.Type.INT);
        Event event = new Event(System.currentTimeMillis(), new Object[]{"WSO2", 200, 50});

        metaStreamEvent.addInputDefinition(streamDefinition);
        StreamEventConverter converter = StreamEventConverterFactory.constructEventConverter(metaStreamEvent);
        StreamEventFactory eventPool = new StreamEventFactory(metaStreamEvent);
        StreamEvent newEvent = eventPool.newInstance();
        converter.convertEvent(event, newEvent);

        AssertJUnit.assertTrue(converter instanceof SimpleStreamEventConverter);
        AssertJUnit.assertNull(newEvent.getBeforeWindowData());
        AssertJUnit.assertNull(newEvent.getOnAfterWindowData());
        AssertJUnit.assertEquals(2, newEvent.getOutputData().length);

        AssertJUnit.assertEquals(200, newEvent.getOutputData()[1]);
        AssertJUnit.assertEquals("WSO2", newEvent.getOutputData()[0]);
    }

    @Test
    public void testStreamEventConverter() {
        Attribute price = new Attribute("price", Attribute.Type.DOUBLE);
        Attribute volume = new Attribute("volume", Attribute.Type.INT);
        Attribute symbol = new Attribute("symbol", Attribute.Type.STRING);

        MetaStreamEvent metaStreamEvent = new MetaStreamEvent();
        metaStreamEvent.addData(volume);
        metaStreamEvent.initializeOnAfterWindowData();
        metaStreamEvent.addData(price);
        metaStreamEvent.addOutputData(symbol);
        metaStreamEvent.addOutputData(null);        //complex attribute

        StreamDefinition streamDefinition = StreamDefinition.id("cseEventStream").attribute("symbol", Attribute.Type
                .STRING).attribute("price", Attribute.Type.DOUBLE).attribute("volume", Attribute.Type.INT);
        Event event = new Event(System.currentTimeMillis(), new Object[]{"WSO2", 200, 50});

        metaStreamEvent.addInputDefinition(streamDefinition);
        StreamEventConverter converter = StreamEventConverterFactory.constructEventConverter(metaStreamEvent);
        StreamEventFactory eventPool = new StreamEventFactory(metaStreamEvent);

        StreamEvent newEvent = eventPool.newInstance();
        converter.convertEvent(event, newEvent);

        AssertJUnit.assertTrue(converter instanceof SelectiveStreamEventConverter);
        AssertJUnit.assertEquals(1, newEvent.getBeforeWindowData().length);      //volume
        AssertJUnit.assertEquals(1, newEvent.getOnAfterWindowData().length);     //price
        AssertJUnit.assertEquals(2, newEvent.getOutputData().length);            //symbol and avgPrice

        AssertJUnit.assertEquals(50, newEvent.getBeforeWindowData()[0]);
        AssertJUnit.assertEquals(200, newEvent.getOnAfterWindowData()[0]);
        AssertJUnit.assertEquals("WSO2", newEvent.getOutputData()[0]);
    }

    @Test
    public void testExpressionExecutors() {
//        StreamDefinition streamDefinition = StreamDefinition.id("cseEventStream").attribute("symbol", Attribute
// .Type.STRING).attribute("price", Attribute.Type.FLOAT).attribute("volume", Attribute.Type.INT);

        VariableExpressionExecutor priceVariableExpressionExecutor = new VariableExpressionExecutor(new Attribute
                ("price", Attribute.Type.FLOAT), 0, 0);
        priceVariableExpressionExecutor.setPosition(new int[]{0, SiddhiConstants.UNKNOWN_STATE, SiddhiConstants
                .OUTPUT_DATA_INDEX, 1});

        ExpressionExecutor addExecutor = new AddExpressionExecutorFloat(new ConstantExpressionExecutor(10f, Attribute
                .Type.FLOAT), priceVariableExpressionExecutor);

        StreamEvent event = new StreamEvent(0, 0, 3);
        event.setOutputData(new Object[]{"WSO2", 10f, 5});

        AssertJUnit.assertEquals("Result of adding should be 20.0", 20f, addExecutor.execute(event));
    }

    @Test
    public void testConditionExpressionExecutors() {
//        StreamDefinition streamDefinition = StreamDefinition.id("cseEventStream").attribute("symbol", Attribute
// .Type.STRING).attribute("price", Attribute.Type.FLOAT).attribute("volume", Attribute.Type.INT);

        VariableExpressionExecutor priceVariableExpressionExecutor = new VariableExpressionExecutor(new Attribute
                ("price", Attribute.Type.FLOAT), 0, 0);
        priceVariableExpressionExecutor.setPosition(new int[]{0, SiddhiConstants.UNKNOWN_STATE, SiddhiConstants
                .OUTPUT_DATA_INDEX, 1});

        VariableExpressionExecutor volumeVariableExpressionExecutor = new VariableExpressionExecutor(new Attribute
                ("volume", Attribute.Type.INT), 0, 0);
        volumeVariableExpressionExecutor.setPosition(new int[]{0, SiddhiConstants.UNKNOWN_STATE, SiddhiConstants
                .OUTPUT_DATA_INDEX, 2});

        ExpressionExecutor compareLessThanExecutor = new LessThanCompareConditionExpressionExecutorFloatFloat(new
                ConstantExpressionExecutor(10f, Attribute.Type.FLOAT), priceVariableExpressionExecutor);
        ExpressionExecutor compareGreaterThanExecutor = new GreaterThanCompareConditionExpressionExecutorIntInt(new
                ConstantExpressionExecutor(10, Attribute.Type.INT), volumeVariableExpressionExecutor);
        ExpressionExecutor andExecutor = new AndConditionExpressionExecutor(compareLessThanExecutor,
                compareGreaterThanExecutor);

        int count = 0;
        for (int i = 0; i < 3; i++) {
            StreamEvent event = new StreamEvent(0, 0, 3);
            event.setOutputData(new Object[]{"WSO2", i * 11f, 5});
            if ((Boolean) andExecutor.execute(event)) {
                count++;
            }
        }

        AssertJUnit.assertEquals("Two events should pass through executor", 2, count);
    }

    @Test(expectedExceptions = OperationNotSupportedException.class)
    public void testConditionExpressionExecutorValidation() {
//        StreamDefinition streamDefinition = StreamDefinition.id("cseEventStream").attribute("symbol", Attribute
// .Type.STRING).attribute("price", Attribute.Type.FLOAT).attribute("volume", Attribute.Type.INT);

        VariableExpressionExecutor volumeVariableExpressionExecutor = new VariableExpressionExecutor(new Attribute
                ("volume", Attribute.Type.INT), 0, 0);
        volumeVariableExpressionExecutor.setPosition(new int[]{0, SiddhiConstants.UNKNOWN_STATE, SiddhiConstants
                .OUTPUT_DATA_INDEX, 2});

        ConstantExpressionExecutor constantExpressionExecutor = new ConstantExpressionExecutor(10f, Attribute.Type
                .FLOAT);
        ExpressionExecutor compareGreaterThanExecutor = new GreaterThanCompareConditionExpressionExecutorIntInt(new
                ConstantExpressionExecutor(10, Attribute.Type.INT), volumeVariableExpressionExecutor);
        ExpressionExecutor andExecutor = new AndConditionExpressionExecutor(constantExpressionExecutor,
                compareGreaterThanExecutor);
    }

    @Test
    public void testQueryParser() {
        StreamDefinition streamDefinition = StreamDefinition.id("cseEventStream").attribute("symbol", Attribute.Type
                .STRING).attribute("price", Attribute.Type.FLOAT).attribute("volume", Attribute.Type.INT);
        StreamDefinition outStreamDefinition = StreamDefinition.id("outputStream").attribute("symbol", Attribute.Type
                .STRING).attribute("price", Attribute.Type.FLOAT);
        Query query = new Query();
        query.annotation(Annotation.annotation("info").element("name", "query1"));
        query.from(InputStream.stream("cseEventStream").filter(Expression.compare(Expression.variable("volume"),
                Compare.Operator.NOT_EQUAL, Expression.value(50))));
        query.select(Selector.selector().select("symbol", Expression.variable("symbol")).select("price", Expression
                .variable("price")));
        query.insertInto("outputStream");
        Map<String, AbstractDefinition> tableDefinitionMap = new HashMap<>();
        Map<String, AbstractDefinition> windowDefinitionMap = new HashMap<>();
        Map<String, AbstractDefinition> aggregationDefinitionMap = new HashMap<>();
        Map<String, Table> tableMap = new HashMap<String, Table>();
        Map<String, Window> eventWindowMap = new HashMap<String, Window>();
        Map<String, AggregationRuntime> aggregationMap = new HashMap<String, AggregationRuntime>();
        Map<String, List<Source>> eventSourceMap = new HashMap<String, List<Source>>();
        Map<String, List<Sink>> eventSinkMap = new HashMap<String, List<Sink>>();
        Map<String, AbstractDefinition> streamDefinitionMap = new HashMap<String, AbstractDefinition>();
        LockSynchronizer lockSynchronizer = new LockSynchronizer();
        streamDefinitionMap.put("cseEventStream", streamDefinition);
        streamDefinitionMap.put("outputStream", outStreamDefinition);
        SiddhiContext siddhicontext = new SiddhiContext();
        SiddhiAppContext context = new SiddhiAppContext();
        context.setSiddhiContext(siddhicontext);
        context.setIdGenerator(new IdGenerator());
        context.setSnapshotService(new SnapshotService(context));
        QueryRuntimeImpl runtime = QueryParser.parse(query, context, streamDefinitionMap, tableDefinitionMap,
                windowDefinitionMap, aggregationDefinitionMap, tableMap, aggregationMap, eventWindowMap,
                lockSynchronizer, "1", false, SiddhiConstants.PARTITION_ID_DEFAULT);
        AssertJUnit.assertNotNull(runtime);
        AssertJUnit.assertTrue(runtime.getStreamRuntime() instanceof SingleStreamRuntime);
        AssertJUnit.assertNotNull(runtime.getSelector());
        AssertJUnit.assertTrue(runtime.getMetaComplexEvent() instanceof MetaStreamEvent);
    }

    @Test
    public void testUpdateMetaEvent() {
        StreamDefinition streamDefinition = StreamDefinition.id("cseEventStream").attribute("symbol", Attribute.Type
                .STRING).attribute("price", Attribute.Type.FLOAT).attribute("volume", Attribute.Type.INT);

        Attribute price = new Attribute("price", Attribute.Type.FLOAT);
        Attribute volume = new Attribute("volume", Attribute.Type.INT);
        Attribute symbol = new Attribute("symbol", Attribute.Type.STRING);

        MetaStreamEvent metaStreamEvent = new MetaStreamEvent();
        metaStreamEvent.addData(volume);
        metaStreamEvent.addData(price);
        metaStreamEvent.addData(symbol);
        metaStreamEvent.initializeOnAfterWindowData();
        metaStreamEvent.addData(price);
        metaStreamEvent.addOutputData(symbol);
        metaStreamEvent.addOutputData(null);
        MetaStateEvent metaStateEvent = new MetaStateEvent(1);
        metaStateEvent.addEvent(metaStreamEvent);

        VariableExpressionExecutor priceVariableExpressionExecutor = new VariableExpressionExecutor(new Attribute
                ("price", Attribute.Type.FLOAT), 0, 0);
        VariableExpressionExecutor volumeVariableExpressionExecutor = new VariableExpressionExecutor(new Attribute
                ("volume", Attribute.Type.INT), 0, 0);
        VariableExpressionExecutor symbolVariableExpressionExecutor = new VariableExpressionExecutor(new Attribute
                ("symbol", Attribute.Type.STRING), 0, 0);

        QueryParserHelper.reduceMetaComplexEvent(metaStateEvent);
        QueryParserHelper.updateVariablePosition(metaStateEvent, Arrays.asList(priceVariableExpressionExecutor,
                volumeVariableExpressionExecutor, symbolVariableExpressionExecutor));

        AssertJUnit.assertEquals(1, metaStreamEvent.getBeforeWindowData().size());
        AssertJUnit.assertEquals(1, metaStreamEvent.getOnAfterWindowData().size());
        AssertJUnit.assertEquals(2, metaStreamEvent.getOutputData().size());
        AssertJUnit.assertArrayEquals(new int[]{0, 0, 1, 0}, priceVariableExpressionExecutor.getPosition());
        AssertJUnit.assertArrayEquals(new int[]{0, 0, 0, 0}, volumeVariableExpressionExecutor.getPosition());
        AssertJUnit.assertArrayEquals(new int[]{0, 0, 2, 0}, symbolVariableExpressionExecutor.getPosition());
    }
}
