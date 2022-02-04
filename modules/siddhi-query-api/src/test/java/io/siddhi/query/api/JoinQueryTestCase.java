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
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.stream.InputStream;
import io.siddhi.query.api.execution.query.input.stream.JoinInputStream;
import io.siddhi.query.api.execution.query.output.stream.OutputStream;
import io.siddhi.query.api.execution.query.selection.Selector;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.condition.Compare;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class JoinQueryTestCase {

    @Test
    public void testCreatingJoinQuery() {

        Query query = Query.query().
                annotation(Annotation.annotation("foo").element("name", "Query1").element("summery", "Test Query")
                        .element("Custom")).
                from(
                        InputStream.joinStream(
                                InputStream.stream("s1", "cseEventStream").
                                        window("lengthBatch", Expression.value(50)),
                                JoinInputStream.Type.JOIN,
                                InputStream.stream("s2", "cseEventStream").
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
                                Expression.compare(
                                        Expression.variable("price").ofStream("s1"),
                                        Compare.Operator.EQUAL,
                                        Expression.variable("price").ofStream("s2"))
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
        List<String> streamIds = new ArrayList<>();
        streamIds.add("cseEventStream");
        Assert.assertEquals(query.getInputStream().getAllStreamIds(), streamIds);
        Assert.assertEquals(query.getInputStream().getUniqueStreamIds(), streamIds);
    }

    @Test
    public void testJoinQueryToString() {

        Query query = Query.query().
                annotation(Annotation.annotation("foo").element("name", "Query1").element("summery", "Test Query")
                        .element("Custom")).
                from(
                        InputStream.joinStream(
                                InputStream.stream("s1", "cseEventStream").
                                        window("lengthBatch", Expression.value(50)),
                                JoinInputStream.Type.JOIN,
                                InputStream.stream("s2", "cseEventStream").
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
                                Expression.compare(
                                        Expression.variable("price").ofStream("s1"),
                                        Compare.Operator.EQUAL,
                                        Expression.variable("price").ofStream("s2"))
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
        String queryString = "Query{stream=JoinInputStream{" +
                "leftInputStream=SingleInputStream{isFaultStream=false, isInnerStream=false, id='cseEventStream', " +
                "streamReferenceId='s1', streamHandlers=[Window{namespace='', function='lengthBatch', " +
                "parameters=[IntConstant{value=50}]}], windowPosition=0}, type=JOIN, " +
                "rightInputStream=SingleInputStream{isFaultStream=false, isInnerStream=false, id='cseEventStream', " +
                "streamReferenceId='s2', streamHandlers=[Filter{" +
                "filterExpression=And{leftExpression=Compare{rightExpression=Variable{id='cseEventStream', " +
                "isInnerStream=false, streamIndex=null, functionId='null', functionIndex=null," +
                " attributeName='price'}, operator=GREATER_THAN, leftExpression=Add{leftValue=IntConstant{value=7}, " +
                "rightValue=DoubleConstant{value=9.5}}}, rightExpression=Compare{" +
                "rightExpression=Variable{id='cseEventStream', isInnerStream=false, streamIndex=null, " +
                "functionId='null', functionIndex=null, attributeName='volume'}, operator=GREATER_THAN_EQUAL, " +
                "leftExpression=IntConstant{value=100}}}}, Window{namespace='', function='lengthBatch', " +
                "parameters=[IntConstant{value=50}]}], windowPosition=1}, " +
                "onCompare=Compare{rightExpression=Variable{id='s2', isInnerStream=false, streamIndex=null, " +
                "functionId='null', functionIndex=null, attributeName='price'}, operator=EQUAL, " +
                "leftExpression=Variable{id='s1', isInnerStream=false, streamIndex=null, functionId='null', " +
                "functionIndex=null, attributeName='price'}}, trigger=ALL, within=null, per=null}, " +
                "selector=Selector{selectionList=[OutputAttribute{rename='symbol', " +
                "expression=Variable{id='cseEventStream', isInnerStream=false, streamIndex=null, functionId='null', " +
                "functionIndex=null, attributeName='symbol'}}, OutputAttribute{rename='null', " +
                "expression=Variable{id='cseEventStream', isInnerStream=false, streamIndex=null, functionId='null', " +
                "functionIndex=null, attributeName='symbol'}}], groupByList=[Variable{id='cseEventStream', " +
                "isInnerStream=false, streamIndex=null, functionId='null', functionIndex=null, " +
                "attributeName='symbol'}], havingExpression=Compare{rightExpression=Variable{id='null', " +
                "isInnerStream=false, streamIndex=null, functionId='null', functionIndex=null, " +
                "attributeName='price'}, operator=GREATER_THAN, leftExpression=Add{leftValue=IntConstant{value=7}, " +
                "rightValue=DoubleConstant{value=9.5}}}, orderByList=[], limit=null, offset=null}, " +
                "outputStream=InsertIntoStream{isFaultStream=false, isInnerStream=false}, outputRate=null, " +
                "annotations=[@foo( name = \"Query1\", summery = \"Test Query\", \"Custom\")]}";
        Assert.assertEquals(query.toString(), queryString);
    }

    @Test
    public void testCreatingUnidirectionalJoinQuery() {

        Query.query().
                from(
                        InputStream.joinStream(
                                InputStream.stream("t", "TickEvent"),
                                JoinInputStream.Type.JOIN,
                                InputStream.stream("n", "NewsEvent").
                                        window("unique", Expression.variable("symbol")),
                                Expression.compare(
                                        Expression.variable("symbol").ofStream("t"),
                                        Compare.Operator.EQUAL,
                                        Expression.variable("symbol").ofStream("t")),
                                JoinInputStream.EventTrigger.LEFT
                        )
                ).
                insertInto("JoinInputStream");
    }

    @Test
    public void testCreatingUnidirectionalJoinQuery2() {

        Query.query().
                from(
                        InputStream.joinStream(
                                InputStream.stream("t", "TickEvent"),
                                JoinInputStream.Type.JOIN,
                                InputStream.stream("n", "NewsEvent").
                                        window("unique", Expression.variable("symbol"))
                        )
                ).
                insertInto("JoinInputStream");
    }

    @Test
    public void testCreatingUnidirectionalJoinQuery3() {

        Query.query().
                from(
                        InputStream.joinStream(
                                InputStream.stream("t", "TickEvent"),
                                JoinInputStream.Type.JOIN,
                                InputStream.stream("n", "NewsEvent").
                                        window("unique", Expression.variable("symbol")),
                                JoinInputStream.EventTrigger.LEFT
                        )
                ).
                insertInto("JoinInputStream");
    }

    @Test
    public void testCreatingJoinQuery1() {

        Query query = Query.query().
                annotation(Annotation.annotation("foo").element("name", "Query1").element("summery", "Test Query")
                        .element("Custom")).
                from(
                        InputStream.joinStream(
                                InputStream.stream("s1", "cseEventStream").
                                        window("lengthBatch", Expression.value(50)),
                                JoinInputStream.Type.JOIN,
                                InputStream.stream("s2", "cseEventStream").
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
                                Expression.compare(
                                        Expression.variable("price").ofStream("s1"),
                                        Compare.Operator.EQUAL,
                                        Expression.variable("price").ofStream("s2"))
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

        Query query1 = Query.query().
                annotation(Annotation.annotation("foo").element("name", "Query1").element("summery", "Test Query")
                        .element("Custom")).
                from(
                        InputStream.joinStream(
                                InputStream.stream("s1", "cseEventStream").
                                        window("lengthBatch", Expression.value(50)),
                                JoinInputStream.Type.JOIN,
                                InputStream.stream("s2", "cseEventStream").
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
                                Expression.compare(
                                        Expression.variable("price").ofStream("s1"),
                                        Compare.Operator.EQUAL,
                                        Expression.variable("price").ofStream("s2"))
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
        Assert.assertTrue(query.equals(query1));
        Assert.assertEquals(query.hashCode(), query1.hashCode());
    }
}
