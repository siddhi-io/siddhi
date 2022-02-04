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
import io.siddhi.query.api.exception.DuplicateAttributeException;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import io.siddhi.query.api.exception.UnsupportedAttributeTypeException;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.input.handler.StreamFunction;
import io.siddhi.query.api.execution.query.input.handler.Window;
import io.siddhi.query.api.execution.query.input.state.State;
import io.siddhi.query.api.execution.query.input.stream.InputStream;
import io.siddhi.query.api.execution.query.output.stream.OutputStream;
import io.siddhi.query.api.execution.query.selection.OrderByAttribute;
import io.siddhi.query.api.execution.query.selection.OutputAttribute;
import io.siddhi.query.api.execution.query.selection.Selector;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.Variable;
import io.siddhi.query.api.expression.condition.And;
import io.siddhi.query.api.expression.condition.Compare;
import io.siddhi.query.api.expression.condition.Not;
import io.siddhi.query.api.expression.condition.Or;
import io.siddhi.query.api.expression.constant.IntConstant;
import io.siddhi.query.api.expression.constant.TimeConstant;
import io.siddhi.query.api.expression.math.Add;
import io.siddhi.query.api.expression.math.Divide;
import io.siddhi.query.api.expression.math.Mod;
import io.siddhi.query.api.expression.math.Multiply;
import io.siddhi.query.api.expression.math.Subtract;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class SimpleQueryTestCase {

//    from StockStream[ 7+9.5>price AND 100>=volume]
//    insert into OutStockStream symbol, avg(price) as avgPrice
//    group by symbol
//    having avgPrice>50

    @Test
    public void testCreatingFilterQuery() {

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
        query.insertInto("OutStockStream");

        SiddhiApp.siddhiApp("test").addQuery(query);

    }

    @Test
    public void testQuery1() {

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
                        ).window("length", Expression.value(50))
        );
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("avgPrice", Expression.function("avg", Expression.variable("price"))).
                        groupBy(Expression.variable("symbol")).
                        having(Expression.compare(Expression.variable("avgPrice"),
                                Compare.Operator.GREATER_THAN_EQUAL,
                                Expression.value(50)
                        ))
        );
        query.insertInto("OutStockStream");

    }

    @Test
    public void testQuery2() {

        Query query = Query.query();
        query.from(
                InputStream.stream("StockStream").
                        filter(Expression.and(
                                Expression.compare(
                                        Expression.add(Expression.value(7), Expression.value(9.5)),
                                        Compare.Operator.GREATER_THAN,
                                        Expression.variable("price")),
                                Expression.compare(
                                        Expression.value(100),
                                        Compare.Operator.GREATER_THAN_EQUAL,
                                        Expression.variable("volume")
                                )
                        )).
                        window("length", Expression.value(50)).
                        filter(
                                Expression.compare(
                                        Expression.variable("symbol"),
                                        Compare.Operator.EQUAL,
                                        Expression.value("WSO2")
                                )
                        )
        );
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("avgPrice", Expression.function("avg", Expression.variable("price"))).
                        groupBy(Expression.variable("symbol")).
                        having(Expression.compare(Expression.variable("avgPrice"),
                                Compare.Operator.GREATER_THAN_EQUAL,
                                Expression.value(50)
                        ))
        );
        query.insertInto("OutStockStream");

    }

    @Test
    public void testQuery3() {

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
                        ).
                        function("bar", Expression.value("price")).
                        window("length", Expression.value(50)).
                        function("foo", Expression.value(67), Expression.value(89)).
                        filter(
                                Expression.compare(
                                        Expression.value(10),
                                        Compare.Operator.LESS_THAN_EQUAL,
                                        Expression.variable("price")
                                )
                        )
        );
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("avgPrice", Expression.function("avg", Expression.variable("price"))).
                        groupBy(Expression.variable("symbol")).
                        having(Expression.compare(Expression.variable("avgPrice"),
                                Compare.Operator.GREATER_THAN_EQUAL,
                                Expression.value(50)
                        ))
        );
        query.insertInto("OutStockStream");

    }

    @Test(expectedExceptions = DuplicateAttributeException.class)
    public void testCreatingFilterQueryWithDuplicateOutputAttribute() {

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
                        ).window("lengthBatch", Expression.value(50))
        );
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.function("avg", Expression.variable("price"))).
                        select("price", Expression.variable("price")).
                        groupBy(Expression.variable("symbol")).
                        having(Expression.compare(Expression.variable("avgPrice"),
                                Compare.Operator.GREATER_THAN_EQUAL,
                                Expression.value(50)
                        ))
        );
        query.insertInto("OutStockStream");

    }

//    from (from StockStream[win.length(50)][ price >= 20]
//            return symbol, avg(price) as avgPrice
//    group by symbol) [symbol=="IBM"]
//    insert into IBMOutStockStream symbol, avgPrice

    @Test
    public void testCreatingNestedFilterQuery() {

        Query query = Query.query();
        query.from(InputStream.stream(
                Query.query().
                        from(InputStream.stream("StockStream").
                                filter(
                                        Expression.compare(
                                                Expression.variable("price").ofStream("StockStream"),
                                                Compare.Operator.GREATER_THAN_EQUAL,
                                                Expression.value(20))
                                ).filter(Expression.isNull(Expression.variable("price").ofStream("StockStream")))).
                        select(
                                Selector.selector().
                                        select("symbol", Expression.variable("symbol")).
                                        select("avgPrice", Expression.function("avg", Expression.variable("price")))
                        ).
                        returns())
        );
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("avgPrice", Expression.variable("avgPrice"))
        );
        query.insertInto("IBMOutStockStream");
    }

//    from StockStream[win.lengthBatch(50)][price >= 20]
//    return symbol, avg(price) as avgPrice

    @Test
    public void testCreatingReturnFilterQuery() {

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
                        ).window("lengthBatch", Expression.value(50))
        );
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("avgPrice", Expression.function("avg", Expression.variable("price")))
        );
        query.returns();

    }

    @Test
    public void testCreatingReturnFilterQueryWithExtension() {

        Query query = Query.query();
        query.from(
                InputStream.stream("StockStream").
                        filter(Expression.and(Expression.compare(Expression.function("ext", "FooBarCond", Expression
                                        .value(7), Expression.value(9.5)),
                                Compare.Operator.GREATER_THAN,
                                Expression.variable("price")),
                                Expression.function("ext", "BarCond", Expression.value(100),
                                        Expression.variable("volume")
                                )
                                )
                        ).function("ext", "Foo", Expression.value(67), Expression.value(89)).window("ext",
                        "lengthFirst10", Expression.value(50))
        );
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("avgPrice", Expression.function("ext", "avg", Expression.variable("price")))
        );

    }

    @Test
    public void testCreatingReturnFilterQueryWithFunction() {

        Query query = Query.query();
        query.from(
                InputStream.stream("StockStream").
                        filter(Expression.and(Expression.compare(Expression.function("FooBarCond", Expression.value
                                        (7), Expression.value(9.5)),
                                Compare.Operator.GREATER_THAN,
                                Expression.variable("price")),
                                Expression.function("BarCond", Expression.value(100),
                                        Expression.variable("volume")
                                )
                                )
                        ).function("ext", "Foo", Expression.value(67), Expression.value(89)).window("ext",
                        "lengthFirst10", Expression.value(50))
        );
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("avgPrice", Expression.function("ext", "avg", Expression.variable("symbol")))
        );

    }

    @Test
    public void testCreatingReturnFilterQueryLimitAndSort() {

        Query query = Query.query();
        query.from(
                InputStream.stream("StockStream").
                        filter(Expression.and(Expression.compare(Expression.function("FooBarCond", Expression.value
                                        (7), Expression.value(9.5)),
                                Compare.Operator.GREATER_THAN,
                                Expression.variable("price")),
                                Expression.function("BarCond", Expression.value(100),
                                        Expression.variable("volume")
                                )
                                )
                        ).function("ext", "Foo", Expression.value(67), Expression.value(89)).window("ext",
                        "lengthFirst10", Expression.value(50))
        );
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("avgPrice", Expression.function("ext", "avg", Expression.variable("symbol"))).
                        orderBy(Expression.variable("avgPrice"), OrderByAttribute.Order.DESC).
                        limit(Expression.value(5))
        );

    }

    @Test
    public void testCreatingReturnFilterQueryLimitOffsetAndSort() {

        Query query = Query.query();
        query.from(
                InputStream.stream("StockStream").
                        filter(Expression.and(Expression.compare(Expression.function("FooBarCond", Expression.value
                                        (7), Expression.value(9.5)),
                                Compare.Operator.GREATER_THAN,
                                Expression.variable("price")),
                                Expression.function("BarCond", Expression.value(100),
                                        Expression.variable("volume")
                                )
                                )
                        ).function("ext", "Foo", Expression.value(67), Expression.value(89)).window("ext",
                        "lengthFirst10", Expression.value(50))
        );
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("avgPrice", Expression.function("ext", "avg", Expression.variable("symbol"))).
                        orderBy(Expression.variable("avgPrice"), OrderByAttribute.Order.DESC).
                        limit(Expression.value(5)).offset(Expression.value(2))
        );

    }

    @Test(expectedExceptions = UnsupportedAttributeTypeException.class)
    public void testCreatingReturnFilterQueryLimitAndSortError() {

        Query query = Query.query();
        query.from(
                InputStream.stream("StockStream"));
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        orderBy(Expression.variable("avgPrice")).
                        limit(Expression.value(5.0))
        );

    }

    @Test(expectedExceptions = UnsupportedAttributeTypeException.class)
    public void testCreatingReturnFilterQueryLimitAndSortError2() {

        Query query = Query.query();
        query.from(
                InputStream.stream("StockStream"));
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        orderBy(Expression.variable("avgPrice")).
                        limit(Expression.value(5.0F))
        );

    }

    @Test(expectedExceptions = UnsupportedAttributeTypeException.class)
    public void testCreatingReturnFilterQueryLimitAndSortError3() {

        Query query = Query.query();
        query.from(
                InputStream.stream("StockStream"));
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        orderBy(Expression.variable("avgPrice")).
                        limit(Expression.value(true))
        );

    }

    @Test(expectedExceptions = UnsupportedAttributeTypeException.class)
    public void testCreatingReturnFilterQueryLimitAndSortError4() {

        Query query = Query.query();
        query.from(
                InputStream.stream("StockStream"));
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        orderBy(Expression.variable("avgPrice")).
                        limit(Expression.value("Test"))
        );

    }

    @Test
    public void testCreatingFilterQueryWithFaultStream() {

        Query query = Query.query();
        query.from(
                InputStream.faultStream("StockStream").
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
        query.insertIntoFault("OutStockStream");

        SiddhiApp.siddhiApp("test").addQuery(query);

    }

    @Test
    public void testCreatingFilterQueryWithFaultStream2() {

        Query query = Query.query();
        query.from(
                InputStream.faultStream("e1", "StockStream").
                        filter(
                                Expression.and(
                                        Expression.compare(
                                                Expression.add(Expression.value(7), Expression.value(9.5)),
                                                Compare.Operator.GREATER_THAN,
                                                Expression.variable("price").ofStream("e1")),
                                        Expression.compare(
                                                Expression.value(100),
                                                Compare.Operator.GREATER_THAN_EQUAL,
                                                Expression.variable("volume").ofStream("e1")
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
        query.insertIntoFault("OutStockStream");
        SiddhiApp.siddhiApp("test").addQuery(query);
    }

    @Test
    public void testCreatingFilterQueryWithFaultStream3() {

        Query query = Query.query();
        query.from(
                InputStream.faultStream("StockStream").
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
        query.insertIntoFault("OutStockStream", OutputStream.OutputEventType.CURRENT_EVENTS);
        SiddhiApp.siddhiApp("test").addQuery(query);
    }

    @Test
    public void testCreatingNestedFilterQuery2() {

        Query query = Query.query();
        query.from(InputStream.stream(
                Query.query().
                        from(InputStream.stream("StockStream").
                                filter(
                                        Expression.compare(
                                                Expression.variable("price").ofStream("StockStream"),
                                                Compare.Operator.GREATER_THAN_EQUAL,
                                                Expression.value(20))
                                ).filter(Expression.isNull(Expression.variable("price").ofStream("StockStream")))).
                        select(
                                Selector.selector().
                                        select("symbol", Expression.variable("symbol")).
                                        select("avgPrice", Expression.function("avg", Expression.variable("price")))
                        ).
                        returns(OutputStream.OutputEventType.CURRENT_EVENTS))
        );
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("avgPrice", Expression.variable("avgPrice"))
        );
        query.insertInto("IBMOutStockStream");
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void testSiddhiAppQueryNull() {

        Query query = null;
        SiddhiApp.siddhiApp("test").addQuery(query);
    }

    @Test
    public void testCreatingReturnFilterQueryWithExtension2() {

        Query query = Query.query();
        Window window1 = new Window("ext", "Foo");
        query.from(
                InputStream.stream("StockStream").
                        filter(Expression.and(Expression.compare(Expression.function("ext", "FooBarCond", Expression
                                        .value(7), Expression.value(9.5)),
                                Compare.Operator.GREATER_THAN,
                                Expression.variable("price")),
                                Expression.function("ext", "BarCond", Expression.value(100),
                                        Expression.variable("volume")
                                )
                                )
                        ).function("ext", "Foo", Expression.value(67), Expression.value(89)).
                        window(window1)
        );
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("avgPrice", Expression.function("ext", "avg", Expression.variable("price")))
        );
    }

    @Test
    public void testCreatingReturnFilterQueryWithExtension3() {

        Query query = Query.query();
        Window window1 = new Window("Foo");
        AssertJUnit.assertFalse(window1.equals("falsewindow"));
        query.from(
                InputStream.stream("StockStream").
                        filter(Expression.and(Expression.compare(Expression.function("ext", "FooBarCond", Expression
                                        .value(7), Expression.value(9.5)),
                                Compare.Operator.GREATER_THAN,
                                Expression.variable("price")),
                                Expression.function("ext", "BarCond", Expression.value(100),
                                        Expression.variable("volume")
                                )
                                )
                        ).function("ext", "Foo", Expression.value(67), Expression.value(89)).
                        window(window1)
        );
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("avgPrice", Expression.function("ext", "avg", Expression.variable("price")))
        );
    }

    @Test
    public void testForAttribute() {

        String attributeString = "Attribute{id='symbol', type=STRING}";
        Attribute symbolAttribute = new Attribute("symbol", Attribute.Type.STRING);
        AssertJUnit.assertEquals(attributeString, symbolAttribute.toString());
    }

    @Test
    public void testCreatingFilterQueryWithExpression() {

        Query query = Query.query();

        Variable variable = new Variable("price");
        Variable variable1 = new Variable("symbol");
        variable.setFunctionId("func1");
        variable.setAttributeName("price");
        variable.setFunctionIndex(1);
        variable.setStreamId(true, true, "func");
        variable1.setStreamId(false, true, "func2");

        query.from(
                InputStream.stream("StockStream").
                        filter(Expression.and(Expression.compare(Expression.function("ext",
                                "FooBarCond", Expression.value(7), Expression.value(9.5)),
                                Compare.Operator.GREATER_THAN,
                                Expression.variable(variable.getAttributeName())),
                                Expression.function("ext", "BarCond", Expression.value(100),
                                        Expression.variable("volume")
                                )
                                )
                        ).
                        function("ext", "Foo", Expression.value(67),
                                Expression.value(89)).window("ext", "lengthFirst10", Expression.value(50))
        );
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("avgPrice", Expression.function("ext", "avg",
                                Expression.variable("price")))
        );

        AssertJUnit.assertTrue(variable.isInnerStream());
        AssertJUnit.assertEquals(variable.getFunctionId(), "func1");
        AssertJUnit.assertEquals(Integer.toString(variable.getFunctionIndex()), "1");
    }

    @Test
    public void testQuery4() {

        StreamFunction streamFunction = new StreamFunction("function1");

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
                        ).
                        function(streamFunction).
                        window("length", Expression.value(50)).
                        function("foo", Expression.value(67), Expression.value(89)).
                        filter(
                                Expression.compare(
                                        Expression.value(10),
                                        Compare.Operator.LESS_THAN_EQUAL,
                                        Expression.variable("price")
                                )
                        )
        );
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("avgPrice", Expression.function("avg", Expression.variable("price"))).
                        groupBy(Expression.variable("symbol")).
                        having(Expression.compare(Expression.variable("avgPrice"),
                                Compare.Operator.GREATER_THAN_EQUAL,
                                Expression.value(50)
                        ))
        );
        query.insertInto("OutStockStream");
    }

    @Test
    public void testQuery5() {

        StreamFunction streamFunction = new StreamFunction("funcNameSpace", "function1");

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
                        ).
                        function(streamFunction).
                        window("length", Expression.value(50)).
                        function("foo", Expression.value(67), Expression.value(89)).
                        filter(
                                Expression.compare(
                                        Expression.value(10),
                                        Compare.Operator.LESS_THAN_EQUAL,
                                        Expression.variable("price")
                                )
                        )
        );
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("avgPrice", Expression.function("avg", Expression.variable("price"))).
                        groupBy(Expression.variable("symbol")).
                        having(Expression.compare(Expression.variable("avgPrice"),
                                Compare.Operator.GREATER_THAN_EQUAL,
                                Expression.value(50)
                        ))
        );
        query.insertInto("OutStockStream");
    }

    @Test
    public void testQuery6() {

        Query query = Query.query();
        query.from(
                InputStream.faultStream("StockStream").
                        filter(
                                Expression.isNullFaultStream("StockStream")
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
        query.insertIntoFault("OutStockStream");

        SiddhiApp.siddhiApp("test").addQuery(query);
    }

    @Test
    public void testQuery7() {

        Query query = Query.query();
        query.from(
                InputStream.faultStream("StockStream").
                        filter(
                                Expression.isNullFaultStream("StockStream", 1)
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
        query.insertIntoFault("OutStockStream");

        SiddhiApp.siddhiApp("test").addQuery(query);
    }

    @Test
    public void testQuery8() {

        Query query = Query.query();
        query.from(
                InputStream.faultStream("StockStream").
                        filter(
                                Expression.isNullInnerStream("StockStream", 1)
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
        query.insertIntoFault("OutStockStream");

        SiddhiApp.siddhiApp("test").addQuery(query);
    }

    @Test
    public void testQuery9() {

        Query query = Query.query();
        query.from(
                InputStream.faultStream("StockStream").
                        filter(
                                Expression.isNullInnerStream("StockStream")
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
        query.insertIntoFault("OutStockStream");

        SiddhiApp.siddhiApp("test").addQuery(query);
    }

    @Test
    public void testQuery10() {

        TimeConstant timeConstant = new TimeConstant(1000);

        Query query = Query.query();
        query.from(
                InputStream.patternStream(
                        State.next(
                                State.stream(InputStream.stream("e1", "Stream1")
                                        .filter(Expression.compare(Expression.variable("price"),
                                                Compare.Operator.GREATER_THAN_EQUAL,
                                                Expression.value(30)))),
                                State.next(State.logicalNot(State.stream(InputStream.stream("Stream1")
                                                .filter(Expression.compare(Expression.variable("price"),
                                                        Compare.Operator.GREATER_THAN_EQUAL,
                                                        Expression.value(20)))), timeConstant.week(1)),
                                        State.stream(InputStream.stream("e3", "Stream2").filter(Expression.compare
                                                (Expression.variable("price"),
                                                        Compare.Operator.GREATER_THAN_EQUAL,
                                                        Expression.variable("price").ofStream("e1")))))
                        )
                )
        );
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void testQuery11() {

        Query query = new Query();
        query.from(InputStream.stream("cseEventStream"));
        query.annotation(Annotation.annotation("info").element("name", "query1"));
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("price", Expression.variable("price"))
        );
        query.insertInto("StockQuote");

        SiddhiApp siddhiApp = new SiddhiApp("ep1");
        siddhiApp.addQuery(query);
        siddhiApp.addQuery(query);
    }

    @Test
    public void test1() throws InterruptedException {

        String modString = "Mod{leftValue=IntConstant{value=1}, rightValue=IntConstant{value=2}}";
        String addString = "Add{leftValue=IntConstant{value=1}, rightValue=IntConstant{value=2}}";
        String dividString = "Divide{leftValue=IntConstant{value=1}, rightValue=IntConstant{value=2}}";
        String multiplyString = "Multiply{leftValue=IntConstant{value=1}, rightValue=IntConstant{value=2}}";
        String subtractString = "Minus{leftValue=IntConstant{value=1}, rightValue=IntConstant{value=2}}";

        Mod mod = new Mod(Expression.value(1), Expression.value(2));
        Add add = new Add(Expression.value(1), Expression.value(2));
        Divide divide = new Divide(Expression.value(1), Expression.value(2));
        Multiply multiply = new Multiply(Expression.value(1), Expression.value(2));
        Subtract subtract = new Subtract(Expression.value(1), Expression.value(2));

        AssertJUnit.assertEquals(mod.toString(), modString);
        AssertJUnit.assertEquals(add.toString(), addString);
        AssertJUnit.assertEquals(divide.toString(), dividString);
        AssertJUnit.assertEquals(multiply.toString(), multiplyString);
        AssertJUnit.assertEquals(subtract.toString(), subtractString);
    }

    @Test
    public void test2() {

        String orString = "Or{leftExpression=Compare{rightExpression=DoubleConstant{value=1.02}," +
                " operator=GREATER_THAN, leftExpression=IntConstant{value=2}}, rightExpression=Compare{" +
                "rightExpression=IntConstant{value=3}, operator=GREATER_THAN, leftExpression=IntConstant{value=1}}}";
        String notString = "Not{expression=Compare{rightExpression=IntConstant{value=3}, operator=GREATER_THAN, " +
                "leftExpression=IntConstant{value=1}}}";
        String andString = "And{leftExpression=Compare{rightExpression=DoubleConstant{value=1.02}, " +
                "operator=GREATER_THAN, leftExpression=IntConstant{value=2}}, rightExpression=Compare{rightExpression" +
                "=IntConstant{value=3}, operator=GREATER_THAN, leftExpression=IntConstant{value=1}}}";

        Or or = new Or(Expression.compare(
                Expression.value(2), Compare.Operator.GREATER_THAN, Expression.value(1.02)),
                Expression.compare(Expression.value(1), Compare.Operator.GREATER_THAN, Expression.value(3)));
        Not not = new Not(Expression.compare(Expression.value(1), Compare.Operator.GREATER_THAN, Expression.value(3)));
        And and = new And(Expression.compare(
                Expression.value(2), Compare.Operator.GREATER_THAN, Expression.value(1.02)),
                Expression.compare(Expression.value(1), Compare.Operator.GREATER_THAN, Expression.value(3)));

        AssertJUnit.assertEquals(or.toString(), orString);
        AssertJUnit.assertEquals(and.toString(), andString);
        AssertJUnit.assertEquals(not.toString(), notString);
    }

    @Test
    public void test3() {

        List<OrderByAttribute> list = new ArrayList<>();
        list.add(new OrderByAttribute(Expression.variable("avgPrice"), OrderByAttribute.Order.ASC));

        Query query = Query.query();
        query.from(
                InputStream.stream("StockStream").
                        filter(Expression.and(Expression.compare(Expression.function("FooBarCond", Expression.value
                                        (7), Expression.value(9.5)),
                                Compare.Operator.GREATER_THAN,
                                Expression.variable("price")),
                                Expression.function("BarCond", Expression.value(100),
                                        Expression.variable("volume")
                                )
                                )
                        ).function("ext", "Foo", Expression.value(67), Expression.value(89)).window("ext",
                        "lengthFirst10", Expression.value(50))
        );
        query.select(
                Selector.selector().
                        select("symbol", Expression.variable("symbol")).
                        select("avgPrice", Expression.function("ext", "avg", Expression.variable("symbol"))).
                        addOrderByList(list).
                        limit(Expression.value(5))
        );
    }

    @Test
    public void test4() {

        Selector selector = new Selector();

        List<OutputAttribute> list = new ArrayList<>();
        list.add(new OutputAttribute(Expression.variable("volume")));

        selector.select("symbol", Expression.variable("symbol")).
                orderBy(Expression.variable("symbol"), OrderByAttribute.Order.DESC).
                limit(Expression.value(5)).offset(Expression.value(2)).addSelectionList(list);

        AssertJUnit.assertEquals(IntConstant.value(5), selector.getLimit());
        AssertJUnit.assertEquals(IntConstant.value(2), selector.getOffset());
    }

    @Test(expectedExceptions = DuplicateAttributeException.class)
    public void selectortest2() {

        Selector selector = new Selector();

        List<OutputAttribute> list = new ArrayList<>();
        list.add(new OutputAttribute(Expression.variable("symbol")));

        selector.select("symbol", Expression.variable("symbol")).
                orderBy(Expression.variable("avgPrice"), OrderByAttribute.Order.DESC).
                limit(Expression.value(5)).offset(Expression.value(2)).addSelectionList(list);
    }

    @Test
    public void test5() {

        String selectorString = "Selector{selectionList=[OutputAttribute{rename='symbol', expression=Variable" +
                "{id='null', isInnerStream=false, streamIndex=null, functionId='null', functionIndex=null, " +
                "attributeName='symbol'}}], groupByList=[], havingExpression=null, orderByList=[], limit=null, " +
                "offset=null}";

        String selector = Selector.selector().select(Expression.variable("symbol")).toString();

        AssertJUnit.assertEquals(selector, selectorString);
    }

    @Test
    public void test6() {

        Selector selector = new Selector();
        selector.select("symbol", Expression.isNull(Expression.variable("symbol")));
        selector.select("symbol1", Expression.not(Expression.variable("symbol")));
        selector.select("symbol2", Expression.in(Expression.compare(
                Expression.variable("symbol"), Compare.Operator.EQUAL,
                Expression.variable("foo")), "aTable"));

        Selector selector1 = new Selector();
        selector1.select("symbol", Expression.isNull(Expression.variable("symbol")));
        selector1.select("symbol1", Expression.not(Expression.variable("symbol")));
        selector1.select("symbol2", Expression.in(Expression.compare(
                Expression.variable("symbol"), Compare.Operator.EQUAL,
                Expression.variable("foo")), "aTable"));

        AssertJUnit.assertTrue(selector.equals(selector1));
        AssertJUnit.assertEquals(selector.hashCode(), selector1.hashCode());

        String selectorString = "Selector{selectionList=[OutputAttribute{rename='symbol', " +
                "expression=IsNull{streamId='null', streamIndex=null, isInnerStream=false, " +
                "isFaultStream=false, expression=Variable{id='null', isInnerStream=false, " +
                "streamIndex=null, functionId='null', functionIndex=null, attributeName='symbol'}}}, " +
                "OutputAttribute{rename='symbol1', expression=Not{expression=Variable{id='null', " +
                "isInnerStream=false, streamIndex=null, functionId='null', functionIndex=null, " +
                "attributeName='symbol'}}}, OutputAttribute{rename='symbol2', " +
                "expression=In{expression=Compare{rightExpression=Variable{id='null', " +
                "isInnerStream=false, streamIndex=null, functionId='null', functionIndex=null, " +
                "attributeName='foo'}, operator=EQUAL, leftExpression=Variable{id='null', " +
                "isInnerStream=false, streamIndex=null, functionId='null', functionIndex=null, " +
                "attributeName='symbol'}}, sourceId='aTable'}}], groupByList=[], havingExpression=null, " +
                "orderByList=[], limit=null, offset=null}";

        AssertJUnit.assertEquals(selector.toString(), selectorString);
    }

}
