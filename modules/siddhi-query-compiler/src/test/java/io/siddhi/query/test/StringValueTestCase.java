package io.siddhi.query.test;


import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.expression.constant.StringConstant;
import io.siddhi.query.compiler.SiddhiCompiler;
import io.siddhi.query.compiler.exception.SiddhiParserException;
import org.apache.commons.lang3.StringEscapeUtils;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class StringValueTestCase {
    @Test
    void test1() {
        String literal = "abc";
        String queryStringFormat = "from a select \"\"\"%s\"\"\" as b insert into c";
        Query query = SiddhiCompiler.parseQuery(String.format(queryStringFormat, literal));
        AssertJUnit.assertNotNull(query);
        AssertJUnit.assertNotNull(query.getSelector());
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList());
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList().get(0));
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList().get(0).getExpression());
        AssertJUnit.assertTrue(query.getSelector().getSelectionList().get(0).getExpression() instanceof StringConstant);
        AssertJUnit.assertEquals(literal,
                ((StringConstant) query.getSelector().getSelectionList().get(0).getExpression()).getValue());
    }

    @Test
    void test2() {
        String literal = "a'bc";
        String queryStringFormat = "from a select \"\"\"%s\"\"\" as b insert into c";
        Query query = SiddhiCompiler.parseQuery(String.format(queryStringFormat, literal));
        AssertJUnit.assertNotNull(query);
        AssertJUnit.assertNotNull(query.getSelector());
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList());
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList().get(0));
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList().get(0).getExpression());
        AssertJUnit.assertTrue(query.getSelector().getSelectionList().get(0).getExpression() instanceof StringConstant);
        AssertJUnit.assertEquals(literal,
                ((StringConstant) query.getSelector().getSelectionList().get(0).getExpression()).getValue());
    }

    @Test(expectedExceptions = SiddhiParserException.class)
    void test3() {
        String literal = "a\"bc";
        String queryStringFormat = "from a select \"\"\"%s\"\"\" as b insert into c";
        SiddhiCompiler.parseQuery(String.format(queryStringFormat, literal));
    }

    @Test
    void test4() {
        String literal = "a\"bc";
        String queryStringFormat = "from a select \"\"\"%s\"\"\" as b insert into c";
        Query query = SiddhiCompiler.parseQuery(String.format(queryStringFormat,
                StringEscapeUtils.escapeJava(literal)));
        AssertJUnit.assertNotNull(query);
        AssertJUnit.assertNotNull(query.getSelector());
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList());
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList().get(0));
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList().get(0).getExpression());
        AssertJUnit.assertTrue(query.getSelector().getSelectionList().get(0).getExpression() instanceof StringConstant);
        AssertJUnit.assertEquals(literal,
                ((StringConstant) query.getSelector().getSelectionList().get(0).getExpression()).getValue());
    }

    @Test(expectedExceptions = SiddhiParserException.class)
    void test5() {
        String literal = "abc\"";
        String queryStringFormat = "from a select \"\"\"%s\"\"\" as b insert into c";
        SiddhiCompiler.parseQuery(String.format(queryStringFormat, literal));
    }

    @Test
    void test6() {
        String literal = "abc\"";
        String queryStringFormat = "from a select \"\"\"%s\"\"\" as b insert into c";
        Query query = SiddhiCompiler.parseQuery(String.format(queryStringFormat,
                StringEscapeUtils.escapeJava(literal)));
        AssertJUnit.assertNotNull(query);
        AssertJUnit.assertNotNull(query.getSelector());
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList());
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList().get(0));
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList().get(0).getExpression());
        AssertJUnit.assertTrue(query.getSelector().getSelectionList().get(0).getExpression() instanceof StringConstant);
        AssertJUnit.assertEquals(literal,
                ((StringConstant) query.getSelector().getSelectionList().get(0).getExpression()).getValue());
    }

    @Test(expectedExceptions = SiddhiParserException.class)
    void test7() {
        String literal = "a\"\"\"bc";
        String queryStringFormat = "from a select \"\"\"%s\"\"\" as b insert into c";
        SiddhiCompiler.parseQuery(String.format(queryStringFormat, literal));
    }

    @Test
    void test8() {
        String literal = "a\"\"\"bc";
        String queryStringFormat = "from a select \"\"\"%s\"\"\" as b insert into c";
        Query query = SiddhiCompiler.parseQuery(String.format(queryStringFormat,
                StringEscapeUtils.escapeJava(literal)));
        AssertJUnit.assertNotNull(query);
        AssertJUnit.assertNotNull(query.getSelector());
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList());
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList().get(0));
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList().get(0).getExpression());
        AssertJUnit.assertTrue(query.getSelector().getSelectionList().get(0).getExpression() instanceof StringConstant);
        AssertJUnit.assertEquals(literal,
                ((StringConstant) query.getSelector().getSelectionList().get(0).getExpression()).getValue());
    }

    @Test
    void test9() {
        String literal = "a\\u005cbc";
        String queryStringFormat = "from a select \"\"\"%s\"\"\" as b insert into c";
        Query query = SiddhiCompiler.parseQuery(String.format(queryStringFormat, literal));
        AssertJUnit.assertNotNull(query);
        AssertJUnit.assertNotNull(query.getSelector());
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList());
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList().get(0));
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList().get(0).getExpression());
        AssertJUnit.assertTrue(query.getSelector().getSelectionList().get(0).getExpression() instanceof StringConstant);
        AssertJUnit.assertEquals("a\\bc",
                ((StringConstant) query.getSelector().getSelectionList().get(0).getExpression()).getValue());
    }

    @Test
    void test10() {
        String literal = "abc";
        String queryStringFormat = "from a select \"%s\" as b insert into c";
        Query query = SiddhiCompiler.parseQuery(String.format(queryStringFormat, literal));
        AssertJUnit.assertNotNull(query);
        AssertJUnit.assertNotNull(query.getSelector());
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList());
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList().get(0));
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList().get(0).getExpression());
        AssertJUnit.assertTrue(query.getSelector().getSelectionList().get(0).getExpression() instanceof StringConstant);
        AssertJUnit.assertEquals(literal,
                ((StringConstant) query.getSelector().getSelectionList().get(0).getExpression()).getValue());
    }

    @Test
    void test11() {
        String literal = "abc";
        String queryStringFormat = "from a select '%s' as b insert into c";
        Query query = SiddhiCompiler.parseQuery(String.format(queryStringFormat, literal));
        AssertJUnit.assertNotNull(query);
        AssertJUnit.assertNotNull(query.getSelector());
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList());
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList().get(0));
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList().get(0).getExpression());
        AssertJUnit.assertTrue(query.getSelector().getSelectionList().get(0).getExpression() instanceof StringConstant);
        AssertJUnit.assertEquals(literal,
                ((StringConstant) query.getSelector().getSelectionList().get(0).getExpression()).getValue());
    }

    @Test
    void test12() {
        String literal = "a'bc";
        String queryStringFormat = "from a select \"%s\" as b insert into c";
        Query query = SiddhiCompiler.parseQuery(String.format(queryStringFormat, literal));
        AssertJUnit.assertNotNull(query);
        AssertJUnit.assertNotNull(query.getSelector());
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList());
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList().get(0));
        AssertJUnit.assertNotNull(query.getSelector().getSelectionList().get(0).getExpression());
        AssertJUnit.assertTrue(query.getSelector().getSelectionList().get(0).getExpression() instanceof StringConstant);
        AssertJUnit.assertEquals(literal,
                ((StringConstant) query.getSelector().getSelectionList().get(0).getExpression()).getValue());
    }

    @Test(expectedExceptions = SiddhiParserException.class)
    void test13() {
        String literal = "a'bc";
        String queryStringFormat = "from a select '%s' as b insert into c";
        SiddhiCompiler.parseQuery(String.format(queryStringFormat, literal));
    }

    @Test(expectedExceptions = SiddhiParserException.class)
    void test14() {
        String literal = "a'bc";
        String queryStringFormat = "from a select '%s' as b insert into c";
        SiddhiCompiler.parseQuery(String.format(queryStringFormat, StringEscapeUtils.escapeJava(literal)));
    }

    @Test(expectedExceptions = SiddhiParserException.class)
    void test15() {
        String literal = "a\"bc";
        String queryStringFormat = "from a select \"%s\" as b insert into c";
        SiddhiCompiler.parseQuery(String.format(queryStringFormat, literal));
    }

    @Test(expectedExceptions = SiddhiParserException.class)
    void test16() {
        String literal = "a\"bc";
        String queryStringFormat = "from a select \"%s\" as b insert into c";
        SiddhiCompiler.parseQuery(String.format(queryStringFormat, StringEscapeUtils.escapeJava(literal)));
    }


}
