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


import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.stream.InputStream;
import io.siddhi.query.api.execution.query.output.stream.OutputStream;
import io.siddhi.query.api.execution.query.output.stream.UpdateStream;
import io.siddhi.query.api.execution.query.selection.Selector;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.condition.Compare;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TableQueryTestCase {

    @Test
    public void testCreatingDeleteQuery() {
        Query query = Query.query();
        query.from(
                InputStream.stream("cseEventStream").
                        filter(
                                Expression.and(
                                        Expression.compare(
                                                Expression.add(Expression.value(7), Expression.value(9.5)),
                                                Compare.Operator.GREATER_THAN,
                                                Expression.variable("price")),
                                        Expression.compare(Expression.value(100),
                                                Compare.Operator.GREATER_THAN_EQUAL,
                                                Expression.variable("volume")
                                        )
                                )
                        ).window("lengthBatch", Expression.value(50))
        );
        query.deleteBy("StockQuote", Expression.compare(
                Expression.variable("symbol"),
                Compare.Operator.EQUAL,
                Expression.variable("symbol").ofStream("StockQuote")));

    }


    @Test
    public void testCreatingDeleteByTypeQuery() {
        Query query = Query.query();
        query.from(
                InputStream.stream("cseEventStream").
                        filter(Expression.and(Expression.compare(Expression.add(Expression.value(7), Expression.value
                                        (9.5)),
                                Compare.Operator.GREATER_THAN,
                                Expression.variable("price")),
                                Expression.compare(Expression.value(100),
                                        Compare.Operator.GREATER_THAN_EQUAL,
                                        Expression.variable("volume")
                                )
                                )
                        ).window("lengthBatch", Expression.value(50))
        );
        query.deleteBy("StockQuote", OutputStream.OutputEventType.ALL_EVENTS,
                Expression.compare(
                        Expression.variable("symbol"),
                        Compare.Operator.EQUAL,
                        Expression.variable("symbol").ofStream("StockQuote"))
        );

    }

    @Test
    public void testCreatingUpdateByQuery() {
        Query query = Query.query();
        query.from(
                InputStream.stream("cseEventStream").
                        filter(Expression.and(Expression.compare(Expression.add(Expression.value(7), Expression.value
                                        (9.5)),
                                Compare.Operator.GREATER_THAN,
                                Expression.variable("price")),
                                Expression.compare(Expression.value(100),
                                        Compare.Operator.GREATER_THAN_EQUAL,
                                        Expression.variable("volume")
                                )
                                )
                        ).window("lengthBatch", Expression.value(50))
        );
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("price"))
        );
        query.updateBy("StockQuote", Expression.compare(
                Expression.variable("symbol"),
                Compare.Operator.EQUAL,
                Expression.variable("symbol").ofStream("StockQuote")));
    }

    @Test
    public void testCreatingUpdateByTypeQuery() {
        Query query = Query.query();
        query.from(
                InputStream.stream("cseEventStream").
                        filter(Expression.and(Expression.compare(Expression.add(Expression.value(7), Expression.value
                                        (9.5)),
                                Compare.Operator.GREATER_THAN,
                                Expression.variable("price")),
                                Expression.compare(Expression.value(100),
                                        Compare.Operator.GREATER_THAN_EQUAL,
                                        Expression.variable("volume")
                                )
                                )
                        ).window("lengthBatch", Expression.value(50))
        );
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("price")).
                        select("volume", Expression.variable("volume"))
        );
        query.updateBy("StockQuote", OutputStream.OutputEventType.ALL_EVENTS,
                UpdateStream.updateSet().
                        set(
                                Expression.variable("price").ofStream("StockQuote"),
                                Expression.variable("price")).
                        set(
                                Expression.variable("volume").ofStream("StockQuote"),
                                Expression.variable("volume")),
                Expression.compare(
                        Expression.variable("symbol"),
                        Compare.Operator.EQUAL,
                        Expression.variable("symbol").ofStream("StockQuote")));

    }


    @Test
    public void testCreatingInQuery() {
        Query query = Query.query();
        query.from(
                InputStream.stream("cseEventStream").
                        filter(
                                Expression.and(
                                        Expression.compare(
                                                Expression.add(Expression.value(7), Expression.value(9.5)),
                                                Compare.Operator.GREATER_THAN,
                                                Expression.variable("price")),
                                        Expression.in(
                                                Expression.compare(Expression.value(9.5),
                                                        Compare.Operator.GREATER_THAN,
                                                        Expression.variable("price")),
                                                "table"
                                        )
                                )
                        ).window("lengthBatch", Expression.value(50))
        );
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("avgPrice", Expression.function("avg", Expression.variable("symbol"))).
                        groupBy(Expression.variable("symbol")).
                        having(Expression.compare(Expression.variable("avgPrice"),
                                Compare.Operator.GREATER_THAN_EQUAL,
                                Expression.value(50)
                        ))
        );
        query.insertInto("StockQuote");

    }

    @Test
    public void testQuery1() {
        Query query = Query.query();
        query.from(
                InputStream.stream("StockStream").
                        filter(Expression.and(Expression.compare(Expression.add(Expression.value(7), Expression.value
                                        (9.5)),
                                Compare.Operator.GREATER_THAN,
                                Expression.variable("price")),
                                Expression.compare(Expression.value(100),
                                        Compare.Operator.GREATER_THAN_EQUAL,
                                        Expression.variable("volume")
                                )
                                )
                        ).window("length", Expression.value(50))
        );
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("price")).
                        select("volume", Expression.variable("volume"))
        );
        query.updateOrInsertBy("StockQuote",
                UpdateStream.updateSet().
                        set(
                                Expression.variable("price").ofStream("StockQuote"),
                                Expression.variable("price")).
                        set(
                                Expression.variable("volume").ofStream("StockQuote"),
                                Expression.variable("volume")),
                Expression.compare(
                        Expression.variable("symbol"),
                        Compare.Operator.EQUAL,
                        Expression.variable("symbol").ofStream("StockQuote")));

        Query query1 = Query.query();
        query1.from(
                InputStream.stream("StockStream").
                        filter(Expression.and(Expression.compare(Expression.add(Expression.value(7), Expression.value
                                        (9.5)),
                                Compare.Operator.GREATER_THAN,
                                Expression.variable("price")),
                                Expression.compare(Expression.value(100),
                                        Compare.Operator.GREATER_THAN_EQUAL,
                                        Expression.variable("volume")
                                )
                                )
                        ).window("length", Expression.value(50))
        );
        query1.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("price")).
                        select("volume", Expression.variable("volume"))
        );
        query1.updateOrInsertBy("StockQuote",
                UpdateStream.updateSet().
                        set(
                                Expression.variable("price").ofStream("StockQuote"),
                                Expression.variable("price")).
                        set(
                                Expression.variable("volume").ofStream("StockQuote"),
                                Expression.variable("volume")),
                Expression.compare(
                        Expression.variable("symbol"),
                        Compare.Operator.EQUAL,
                        Expression.variable("symbol").ofStream("StockQuote")));
        Assert.assertTrue(query.equals(query1));
        Assert.assertEquals(query.hashCode(), query1.hashCode());
    }

    @Test
    public void testQuery2() {
        Query query = Query.query();
        query.from(
                InputStream.stream("cseEventStream").
                        filter(
                                Expression.and(
                                        Expression.compare(
                                                Expression.add(Expression.value(7), Expression.value(9.5)),
                                                Compare.Operator.GREATER_THAN,
                                                Expression.variable("price")),
                                        Expression.compare(Expression.value(100),
                                                Compare.Operator.GREATER_THAN_EQUAL,
                                                Expression.variable("volume")
                                        )
                                )
                        ).window("lengthBatch", Expression.value(50))
        );
        query.deleteBy("StockQuote", Expression.compare(
                Expression.variable("symbol"),
                Compare.Operator.EQUAL,
                Expression.variable("symbol").ofStream("StockQuote")));

        Query query2 = Query.query();
        query2.from(
                InputStream.stream("cseEventStream").
                        filter(
                                Expression.and(
                                        Expression.compare(
                                                Expression.add(Expression.value(7), Expression.value(9.5)),
                                                Compare.Operator.GREATER_THAN,
                                                Expression.variable("price")),
                                        Expression.compare(Expression.value(100),
                                                Compare.Operator.GREATER_THAN_EQUAL,
                                                Expression.variable("volume")
                                        )
                                )
                        ).window("lengthBatch", Expression.value(50))
        );
        query2.deleteBy("StockQuote", Expression.compare(
                Expression.variable("symbol"),
                Compare.Operator.EQUAL,
                Expression.variable("symbol").ofStream("StockQuote")));
        Assert.assertTrue(query.equals(query2));
        Assert.assertEquals(query.hashCode(), query2.hashCode());
    }
}
