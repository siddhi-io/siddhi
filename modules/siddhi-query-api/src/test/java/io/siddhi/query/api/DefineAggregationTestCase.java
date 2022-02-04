/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import io.siddhi.query.api.aggregation.TimePeriod;
import io.siddhi.query.api.aggregation.Within;
import io.siddhi.query.api.definition.AggregationDefinition;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.exception.DuplicateDefinitionException;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.store.InputStore;
import io.siddhi.query.api.execution.query.input.stream.InputStream;
import io.siddhi.query.api.execution.query.input.stream.JoinInputStream;
import io.siddhi.query.api.execution.query.output.stream.OutputStream;
import io.siddhi.query.api.execution.query.selection.Selector;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.condition.Compare;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class DefineAggregationTestCase {

    @Test
    public void testDefineAggregationWithTimeRage() {

        AggregationDefinition aggregationDefinition = AggregationDefinition.id("StockAggregation")
                .from(InputStream.stream("StockStream"))
                .select(Selector.basicSelector()
                        .select("timestamp", Expression.variable("timestamp").ofStream("StockStream"))
                        .select("symbol", Expression.variable("symbol").ofStream("StockStream"))
                        .select("price", Expression.variable("price").ofStream("StockStream"))
                        .groupBy(Expression.variable("price").ofStream("StockStream")))
                .aggregateBy(Expression.variable("timestamp"))
                .every(TimePeriod.range(TimePeriod.Duration.SECONDS, TimePeriod.Duration.DAYS));
    }

    @Test
    public void testDefineAggregationWithExactTimeSpecifier() {

        AggregationDefinition aggregationDefinition = AggregationDefinition.id("StockAggregation")
                .from(InputStream.stream("StockStream"))
                .select(Selector.basicSelector()
                        .select("timestamp", Expression.variable("timestamp").ofStream("StockStream"))
                        .select("symbol", Expression.variable("symbol").ofStream("StockStream"))
                        .select("price", Expression.variable("price").ofStream("StockStream"))
                        .groupBy(Expression.variable("price").ofStream("StockStream")))
                .aggregateBy(Expression.variable("timestamp"))
                .every(TimePeriod.interval(TimePeriod.Duration.SECONDS,
                        TimePeriod.Duration.MINUTES,
                        TimePeriod.Duration.HOURS));
    }

    @Test
    public void testAggregationJoin() {

        Query.query().
                from(
                        InputStream.joinStream(
                                InputStream.stream("s1", "cseEventStream").
                                        filter(Expression.and(
                                                Expression.compare(
                                                        Expression.add(Expression.value(7), Expression.value(9.5)),
                                                        Compare.Operator.GREATER_THAN,
                                                        Expression.variable("price").ofStream("cseEventStream")),
                                                Expression.compare(Expression.value(100),
                                                        Compare.Operator.GREATER_THAN_EQUAL,
                                                        Expression.variable("volume").ofStream("cseEventStream")
                                                )
                                                )
                                        ).window("lengthBatch", Expression.value(50)),
                                JoinInputStream.Type.JOIN,
                                InputStore.store("s2", "StockAggregation"),
                                Expression.compare(
                                        Expression.variable("price").ofStream("s1"),
                                        Compare.Operator.EQUAL,
                                        Expression.variable("price").ofStream("s2")),
                                Within.within(Expression.value("2014-02-15T00:00:00Z"),
                                        Expression.value("2014-03-16T00:00:00Z")),
                                Expression.value("day")
                        )
                ).
                select(
                        Selector.selector().
                                select("symbol", Expression.variable("symbol").ofStream("cseEventStream")).
                                select(null, Expression.variable("symbol").ofStream("cseEventStream")).
                                groupBy(Expression.variable("symbol").ofStream("cseEventStream")).
                                having(
                                        Expression.compare(
                                                Expression.add(Expression.value(7), Expression.value(9.5)),
                                                Compare.Operator.GREATER_THAN,
                                                Expression.variable("price"))
                                )
                ).
                insertInto("StockQuote", OutputStream.OutputEventType.EXPIRED_EVENTS);

    }

    @Test
    public void testAggregationJoin1() {

        Query.query().
                from(
                        InputStream.joinStream(
                                InputStream.stream("s1", "cseEventStream").
                                        filter(Expression.and(
                                                Expression.compare(
                                                        Expression.add(Expression.value(7), Expression.value(9.5)),
                                                        Compare.Operator.GREATER_THAN,
                                                        Expression.variable("price").ofStream("cseEventStream")),
                                                Expression.compare(Expression.value(100),
                                                        Compare.Operator.GREATER_THAN_EQUAL,
                                                        Expression.variable("volume").ofStream("cseEventStream")
                                                )
                                                )
                                        ).window("lengthBatch", Expression.value(50)),
                                JoinInputStream.Type.JOIN,
                                InputStore.store("s2", "StockAggregation"),
                                Within.within(Expression.value("2014-02-15T00:00:00Z"),
                                        Expression.value("2014-03-16T00:00:00Z")),
                                Expression.value("day")
                        )
                ).
                select(
                        Selector.selector().
                                select("symbol", Expression.variable("symbol").ofStream("cseEventStream")).
                                select(null, Expression.variable("symbol").ofStream("cseEventStream")).
                                groupBy(Expression.variable("symbol").ofStream("cseEventStream")).
                                having(
                                        Expression.compare(
                                                Expression.add(Expression.value(7), Expression.value(9.5)),
                                                Compare.Operator.GREATER_THAN,
                                                Expression.variable("price"))
                                )
                ).
                insertInto("StockQuote", OutputStream.OutputEventType.EXPIRED_EVENTS);
    }

    @Test
    public void testAggregationJoin2() {

        Query.query().
                from(
                        InputStream.joinStream(
                                InputStream.stream("s1", "cseEventStream").
                                        filter(Expression.and(
                                                Expression.compare(
                                                        Expression.add(Expression.value(7), Expression.value(9.5)),
                                                        Compare.Operator.GREATER_THAN,
                                                        Expression.variable("price").ofStream("cseEventStream")),
                                                Expression.compare(Expression.value(100),
                                                        Compare.Operator.GREATER_THAN_EQUAL,
                                                        Expression.variable("volume").ofStream("cseEventStream")
                                                )
                                                )
                                        ).window("lengthBatch", Expression.value(50)),
                                JoinInputStream.Type.JOIN,
                                InputStore.store("s2", "StockAggregation"),
                                JoinInputStream.EventTrigger.LEFT,
                                Within.within(Expression.value("2014-02-15T00:00:00Z"),
                                        Expression.value("2014-03-16T00:00:00Z")),
                                Expression.value("day")
                        )
                ).
                select(
                        Selector.selector().
                                select("symbol", Expression.variable("symbol").ofStream("cseEventStream")).
                                select(null, Expression.variable("symbol").ofStream("cseEventStream")).
                                groupBy(Expression.variable("symbol").ofStream("cseEventStream")).
                                having(
                                        Expression.compare(
                                                Expression.add(Expression.value(7), Expression.value(9.5)),
                                                Compare.Operator.GREATER_THAN,
                                                Expression.variable("price"))
                                )
                ).
                insertInto("StockQuote", OutputStream.OutputEventType.EXPIRED_EVENTS);
    }

    @Test
    public void testDefineAggregationWithExactTimeSpecifier2() {

        AggregationDefinition aggregationDefinition = AggregationDefinition.id("StockAggregation")
                .from(InputStream.stream("StockStream"))
                .select(Selector.basicSelector()
                        .select("timestamp", Expression.variable("timestamp").ofStream("StockStream"))
                        .select("symbol", Expression.variable("symbol").ofStream("StockStream"))
                        .select("price", Expression.variable("price").ofStream("StockStream"))
                        .groupBy(Expression.variable("price").ofStream("StockStream")))
                .aggregateBy(Expression.variable("timestamp"))
                .every(TimePeriod.interval(TimePeriod.Duration.DAYS,
                        TimePeriod.Duration.MONTHS,
                        TimePeriod.Duration.YEARS));
    }

    @Test
    public void testdefineAggregation() {

        SiddhiApp.siddhiApp("test").defineAggregation(AggregationDefinition.id("stockAggregation")).defineStream(
                StreamDefinition.id("StockStream").attribute("symbol",
                        Attribute.Type.STRING).attribute("price", Attribute.Type.INT).attribute("volume",
                        Attribute.Type.FLOAT));
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void testdefineAggregation2() {

        SiddhiApp.siddhiApp("test").defineAggregation(null).defineStream(
                StreamDefinition.id("StockStream").attribute("symbol",
                        Attribute.Type.STRING).attribute("price", Attribute.Type.INT).attribute("volume",
                        Attribute.Type.FLOAT));
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void testdifineAggregation3() {

        SiddhiApp.siddhiApp("test").defineAggregation(AggregationDefinition.id(null)).defineStream(
                StreamDefinition.id("StockStream").attribute("symbol",
                        Attribute.Type.STRING).attribute("price", Attribute.Type.INT).attribute("volume",
                        Attribute.Type.FLOAT));
    }

    @Test(expectedExceptions = DuplicateDefinitionException.class)
    public void testDefineAggregation4() {

        SiddhiApp.siddhiApp("Test").defineAggregation(AggregationDefinition.id("stockAggregation"))
                .defineStream(StreamDefinition.id("stockAggregation").attribute("symbol",
                        Attribute.Type.STRING).attribute("price", Attribute.Type.INT).
                        attribute("volume", Attribute.Type.FLOAT));
    }

    @Test
    public void testDefineAggregationWithExactTimeSpecifier3() {

        String basicSingleInputStream = "SingleInputStream{isFaultStream=false, isInnerStream=false, " +
                "id='StockStream', streamReferenceId='null', streamHandlers=[], windowPosition=-1}";

        AggregationDefinition aggregationDefinition = AggregationDefinition.id("StockAggregation")
                .from(InputStream.stream("StockStream"))
                .select(Selector.basicSelector()
                        .select("timestamp", Expression.variable("timestamp").ofStream("StockStream"))
                        .select("symbol", Expression.variable("symbol").ofStream("StockStream"))
                        .select("price", Expression.variable("price").ofStream("StockStream"))
                        .groupBy(Expression.variable("price").ofStream("StockStream")));

        AssertJUnit.assertEquals(basicSingleInputStream, aggregationDefinition.getBasicSingleInputStream().toString());
    }

    @Test
    public void testDefineAggregation5() {

        AggregationDefinition aggregationDefinition = AggregationDefinition.id("StockAggregation")
                .from(InputStream.stream("StockStream"))
                .select(Selector.basicSelector()
                        .select("timestamp", Expression.variable("timestamp").ofStream("StockStream"))
                        .select("symbol", Expression.variable("symbol").ofStream("StockStream"))
                        .select("price", Expression.variable("price").ofStream("StockStream"))
                        .groupBy(Expression.variable("price").ofStream("StockStream")));

        Assert.assertEquals(aggregationDefinition.getAttributeNameArray(), new String[]{});
    }

    @Test
    public void testDefineAggregation6() {

        AggregationDefinition aggregationDefinition = AggregationDefinition.id("StockAggregation")
                .from(InputStream.stream("StockStream"))
                .select(Selector.basicSelector()
                        .select("timestamp", Expression.variable("timestamp").ofStream("StockStream"))
                        .select("symbol", Expression.variable("symbol").ofStream("StockStream"))
                        .select("price", Expression.variable("price").ofStream("StockStream"))
                        .groupBy(Expression.variable("price").ofStream("StockStream")));

        AggregationDefinition aggregationDefinition1 = AggregationDefinition.id("StockAggregation")
                .from(InputStream.stream("StockStream"))
                .select(Selector.basicSelector()
                        .select("timestamp", Expression.variable("timestamp").ofStream("StockStream"))
                        .select("symbol", Expression.variable("symbol").ofStream("StockStream"))
                        .select("price", Expression.variable("price").ofStream("StockStream"))
                        .groupBy(Expression.variable("price").ofStream("StockStream")));

        Assert.assertEquals(aggregationDefinition, aggregationDefinition1);
        Assert.assertEquals(aggregationDefinition.hashCode(), aggregationDefinition1.hashCode());
        Assert.assertTrue(aggregationDefinition.equals(aggregationDefinition1));
    }
}
