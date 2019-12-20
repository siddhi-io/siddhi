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
import io.siddhi.query.api.execution.partition.Partition;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.stream.InputStream;
import io.siddhi.query.api.execution.query.selection.Selector;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.condition.Compare;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PartitionQueryTestCase {

//    from StockStream[ 7+9.5>price AND 100>=volume]
//    insert into OutStockStream symbol, avg(price) as avgPrice
//    group by symbol
//    having avgPrice>50

    @Test
    public void testCreatingFilterQuery() {

        Partition partition = Partition.partition().
                with("StockStream", Expression.variable("symbol")).
                with("StockStream1", Expression.variable("symbol")).
                with("StockStream2",
                        Partition.range("LessValue",
                                Expression.compare(
                                        Expression.value(7),
                                        Compare.Operator.GREATER_THAN,
                                        Expression.variable("price"))
                        ),
                        Partition.range("HighValue",
                                Expression.compare(
                                        Expression.value(9.5),
                                        Compare.Operator.LESS_THAN,
                                        Expression.variable("price1"))
                        )
                );

        Query query = Query.query();
        query.from(
                InputStream.stream("StockStream").
                        filter(
                                Expression.and(
                                        Expression.compare(
                                                Expression.add(Expression.value(7), Expression.value(9.5)),
                                                Compare.Operator.GREATER_THAN,
                                                Expression.variable("price")),
                                        Expression.compare(
                                                Expression.value(100),
                                                Compare.Operator.GREATER_THAN_EQUAL,
                                                Expression.variable("volume")
                                        )
                                )
                        )
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
        query.insertIntoInner("OutStockStream");

        partition.addQuery(query);

    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void testPartitionQueryNull() {
        Partition partition = Partition.partition();
        Query query = null;
        partition.addQuery(query);
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void testDuplicatePartitionQuery() {
        Partition partition = Partition.partition().
                with("StockStream", Expression.variable("symbol")).
                with("StockStream", Expression.variable("symbol")).
                with("StockStream2",
                        Partition.range("LessValue",
                                Expression.compare(
                                        Expression.value(7),
                                        Compare.Operator.GREATER_THAN,
                                        Expression.variable("price"))
                        ),
                        Partition.range("HighValue",
                                Expression.compare(
                                        Expression.value(9.5),
                                        Compare.Operator.LESS_THAN,
                                        Expression.variable("price1"))
                        )
                );
    }

    @Test
    public void testQuery1() {

        Partition partition = Partition.partition().
                with("StockStream", Expression.variable("symbol")).
                with("StockStream1", Expression.variable("symbol")).
                with("StockStream2",
                        Partition.range("LessValue",
                                Expression.compare(
                                        Expression.value(7),
                                        Compare.Operator.GREATER_THAN,
                                        Expression.variable("price"))
                        ),
                        Partition.range("HighValue",
                                Expression.compare(
                                        Expression.value(9.5),
                                        Compare.Operator.LESS_THAN,
                                        Expression.variable("price1"))
                        )
                );

        Query query = Query.query();
        query.from(
                InputStream.stream("StockStream").
                        filter(
                                Expression.and(
                                        Expression.compare(
                                                Expression.add(Expression.value(7), Expression.value(9.5)),
                                                Compare.Operator.GREATER_THAN,
                                                Expression.variable("price")),
                                        Expression.compare(
                                                Expression.value(100),
                                                Compare.Operator.GREATER_THAN_EQUAL,
                                                Expression.variable("volume")
                                        )
                                )
                        )
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
        query.insertIntoInner("OutStockStream");

        partition.addQuery(query);

        Partition partition1 = Partition.partition().
                with("StockStream", Expression.variable("symbol")).
                with("StockStream1", Expression.variable("symbol")).
                with("StockStream2",
                        Partition.range("LessValue",
                                Expression.compare(
                                        Expression.value(7),
                                        Compare.Operator.GREATER_THAN,
                                        Expression.variable("price"))
                        ),
                        Partition.range("HighValue",
                                Expression.compare(
                                        Expression.value(9.5),
                                        Compare.Operator.LESS_THAN,
                                        Expression.variable("price1"))
                        )
                );

        Query query1 = Query.query();
        query1.from(
                InputStream.stream("StockStream").
                        filter(
                                Expression.and(
                                        Expression.compare(
                                                Expression.add(Expression.value(7), Expression.value(9.5)),
                                                Compare.Operator.GREATER_THAN,
                                                Expression.variable("price")),
                                        Expression.compare(
                                                Expression.value(100),
                                                Compare.Operator.GREATER_THAN_EQUAL,
                                                Expression.variable("volume")
                                        )
                                )
                        )
        );
        query1.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("avgPrice", Expression.function("avg", Expression.variable("symbol"))).
                        groupBy(Expression.variable("symbol")).
                        having(Expression.compare(Expression.variable("avgPrice"),
                                Compare.Operator.GREATER_THAN_EQUAL,
                                Expression.value(50)
                        ))
        );
        query1.insertIntoInner("OutStockStream");

        partition1.addQuery(query1);

        Assert.assertTrue(partition.equals(partition1));
        Assert.assertEquals(partition.hashCode(), partition1.hashCode());

    }
}
