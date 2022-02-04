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

import io.siddhi.query.api.aggregation.Within;
import io.siddhi.query.api.execution.query.OnDemandQuery;
import io.siddhi.query.api.execution.query.StoreQuery;
import io.siddhi.query.api.execution.query.input.store.InputStore;
import io.siddhi.query.api.execution.query.output.stream.UpdateStream;
import io.siddhi.query.api.execution.query.selection.Selector;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.condition.Compare;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class OnDemandQueryTestCase {

    @Test
    public void test1() {

        StoreQuery.query().
                from(
                        InputStore.store("cseEventTable")).
                select(
                        Selector.selector().
                                select("symbol", Expression.variable("symbol")).
                                select(Expression.variable("price")).
                                groupBy(Expression.variable("symbol")).
                                having(
                                        Expression.compare(
                                                Expression.add(Expression.value(7), Expression.value(9.5)),
                                                Compare.Operator.GREATER_THAN,
                                                Expression.variable("price"))
                                )
                );
    }

    @Test
    public void test2() {

        StoreQuery.query().
                from(
                        InputStore.store("cseEventTable").
                                on(Expression.compare(Expression.variable("price"),
                                        Compare.Operator.GREATER_THAN, Expression.value(40)))).
                select(
                        Selector.selector().
                                select("symbol", Expression.variable("symbol")).
                                select(Expression.variable("price")).
                                groupBy(Expression.variable("symbol")).
                                having(
                                        Expression.compare(
                                                Expression.add(Expression.value(7), Expression.value(9.5)),
                                                Compare.Operator.GREATER_THAN,
                                                Expression.variable("price"))
                                )
                );
    }

    @Test
    public void test3() {

        StoreQuery.query().
                from(
                        InputStore.store("cseEventTable").
                                on(Expression.compare(Expression.variable("price"),
                                        Compare.Operator.GREATER_THAN, Expression.value(40)),
                                        Within.within(Expression.value("2017/01/*")), Expression.value("day"))).
                select(
                        Selector.selector().
                                select("symbol", Expression.variable("symbol")).
                                select(Expression.variable("price")).
                                groupBy(Expression.variable("symbol")).
                                having(
                                        Expression.compare(
                                                Expression.add(Expression.value(7), Expression.value(9.5)),
                                                Compare.Operator.GREATER_THAN,
                                                Expression.variable("price"))
                                )
                );
    }

    @Test
    public void test4() {

        StoreQuery storeQuery = StoreQuery.query();
        storeQuery.
                from(
                        InputStore.store("StockTable")).
                select(
                        Selector.selector().
                                select("symbol", Expression.variable("symbol")).
                                select("price", Expression.variable("price"))
                ).
                updateBy("StockTable", Expression.compare(
                        Expression.variable("symbol"),
                        Compare.Operator.EQUAL,
                        Expression.variable("symbol")));

        AssertJUnit.assertNotNull(storeQuery.toString());
    }

    @Test
    public void test5() {

        StoreQuery storeQuery = StoreQuery.query();
        storeQuery.
                from(
                        InputStore.store("StockTable")).
                select(
                        Selector.selector().
                                select("symbol", Expression.variable("symbol")).
                                select("price", Expression.variable("price"))
                ).
                deleteBy("StockTable", Expression.compare(
                        Expression.variable("symbol"),
                        Compare.Operator.EQUAL,
                        Expression.variable("symbol")));
    }

    @Test
    public void test6() {

        StoreQuery storeQuery = StoreQuery.query();
        storeQuery.
                from(
                        InputStore.store("StockTable")).
                select(
                        Selector.selector().
                                select("symbol", Expression.variable("symbol")).
                                select("price", Expression.variable("price"))
                ).
                updateBy("StockTable", UpdateStream.updateSet().
                        set(
                                Expression.variable("symbol").ofStream("StockStream"),
                                Expression.variable("symbol")).
                        set(
                                Expression.variable("price").ofStream("StockStream"),
                                Expression.variable("price")), Expression.compare(
                        Expression.variable("symbol"),
                        Compare.Operator.EQUAL,
                        Expression.variable("symbol")));
    }

    @Test
    public void test7() {

        OnDemandQuery storeQuery = OnDemandQuery.query();
        storeQuery.
                from(
                        InputStore.store("StockTable")).
                select(
                        Selector.selector().
                                select("symbol", Expression.variable("symbol")).
                                select("price", Expression.variable("price"))
                ).
                updateOrInsertBy("StockTable", UpdateStream.updateSet().
                        set(
                                Expression.variable("symbol").ofStream("StockStream"),
                                Expression.variable("symbol")).
                        set(
                                Expression.variable("pricd").ofStream("StockStream"),
                                Expression.variable("price")), Expression.compare(
                        Expression.variable("symbol"),
                        Compare.Operator.EQUAL,
                        Expression.variable("symbol")));
    }

    @Test
    public void test8() {

        OnDemandQuery.query().
                from(
                        InputStore.store("cseEventTable").
                                on(Within.within(Expression.value("2017/01/*")), Expression.value("day"))).
                select(
                        Selector.selector().
                                select("symbol", Expression.variable("symbol")).
                                select(Expression.variable("price")).
                                groupBy(Expression.variable("symbol")).
                                having(
                                        Expression.compare(
                                                Expression.add(Expression.value(7), Expression.value(9.5)),
                                                Compare.Operator.GREATER_THAN,
                                                Expression.variable("price"))
                                )
                );
    }
}
