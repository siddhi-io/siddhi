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

/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.query.compiler;

import io.siddhi.query.api.SiddhiApp;
import io.siddhi.query.api.definition.AggregationDefinition;
import io.siddhi.query.api.definition.FunctionDefinition;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.execution.partition.Partition;
import io.siddhi.query.api.execution.query.OnDemandQuery;
import io.siddhi.query.api.execution.query.Query;
import io.siddhi.query.api.execution.query.StoreQuery;
import io.siddhi.query.api.expression.Expression;
import io.siddhi.query.api.expression.constant.TimeConstant;
import io.siddhi.query.compiler.exception.SiddhiParserException;
import io.siddhi.query.compiler.internal.SiddhiErrorListener;
import io.siddhi.query.compiler.internal.SiddhiQLBaseVisitorImpl;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Siddhi query compiler
 */
public class SiddhiCompiler {

    public static SiddhiApp parse(String siddhiApp) {

        CharStream input = CharStreams.fromString(siddhiApp);
        SiddhiQLLexer lexer = new SiddhiQLLexer(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(SiddhiErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SiddhiQLParser parser = new SiddhiQLParser(tokens);
        //            parser.setErrorHandler(new BailErrorStrategy());
        parser.removeErrorListeners();
        parser.addErrorListener(SiddhiErrorListener.INSTANCE);
        ParseTree tree = parser.parse();

        SiddhiQLVisitor eval = new SiddhiQLBaseVisitorImpl();
        return (SiddhiApp) eval.visit(tree);
    }

    public static StreamDefinition parseStreamDefinition(String streamDefinition) {

        CharStream input = CharStreams.fromString(streamDefinition);
        SiddhiQLLexer lexer = new SiddhiQLLexer(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(SiddhiErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SiddhiQLParser parser = new SiddhiQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(SiddhiErrorListener.INSTANCE);
        ParseTree tree = parser.definition_stream_final();

        SiddhiQLVisitor eval = new SiddhiQLBaseVisitorImpl();
        return (StreamDefinition) eval.visit(tree);
    }

    public static TableDefinition parseTableDefinition(String tableDefinition) throws SiddhiParserException {

        CharStream input = CharStreams.fromString(tableDefinition);
        SiddhiQLLexer lexer = new SiddhiQLLexer(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(SiddhiErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SiddhiQLParser parser = new SiddhiQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(SiddhiErrorListener.INSTANCE);
        ParseTree tree = parser.definition_table_final();

        SiddhiQLVisitor eval = new SiddhiQLBaseVisitorImpl();
        return (TableDefinition) eval.visit(tree);
    }

    public static AggregationDefinition parseAggregationDefinition(String aggregationDefinition)
            throws SiddhiParserException {

        CharStream input = CharStreams.fromString(aggregationDefinition);
        SiddhiQLLexer lexer = new SiddhiQLLexer(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(SiddhiErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SiddhiQLParser parser = new SiddhiQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(SiddhiErrorListener.INSTANCE);
        ParseTree tree = parser.definition_aggregation_final();

        SiddhiQLVisitor eval = new SiddhiQLBaseVisitorImpl();
        return (AggregationDefinition) eval.visit(tree);
    }

    public static Partition parsePartition(String partition) throws SiddhiParserException {

        CharStream input = CharStreams.fromString(partition);
        SiddhiQLLexer lexer = new SiddhiQLLexer(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(SiddhiErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SiddhiQLParser parser = new SiddhiQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(SiddhiErrorListener.INSTANCE);
        ParseTree tree = parser.partition_final();

        SiddhiQLVisitor eval = new SiddhiQLBaseVisitorImpl();
        return (Partition) eval.visit(tree);
    }

    public static Query parseQuery(String query) throws SiddhiParserException {

        CharStream input = CharStreams.fromString(query);
        SiddhiQLLexer lexer = new SiddhiQLLexer(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(SiddhiErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SiddhiQLParser parser = new SiddhiQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(SiddhiErrorListener.INSTANCE);
        ParseTree tree = parser.query_final();

        SiddhiQLVisitor eval = new SiddhiQLBaseVisitorImpl();
        return (Query) eval.visit(tree);
    }

    public static FunctionDefinition parseFunctionDefinition(String functionDefinition) throws SiddhiParserException {

        CharStream input = CharStreams.fromString(functionDefinition);
        SiddhiQLLexer lexer = new SiddhiQLLexer(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(SiddhiErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SiddhiQLParser parser = new SiddhiQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(SiddhiErrorListener.INSTANCE);
        ParseTree tree = parser.definition_function_final();

        SiddhiQLVisitor eval = new SiddhiQLBaseVisitorImpl();
        return (FunctionDefinition) eval.visit(tree);
    }

    public static TimeConstant parseTimeConstantDefinition(String timeConstantDefinition) throws SiddhiParserException {

        CharStream input = CharStreams.fromString(timeConstantDefinition);
        SiddhiQLLexer lexer = new SiddhiQLLexer(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(SiddhiErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SiddhiQLParser parser = new SiddhiQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(SiddhiErrorListener.INSTANCE);
        ParseTree tree = parser.time_value();

        SiddhiQLVisitor eval = new SiddhiQLBaseVisitorImpl();
        return (TimeConstant) eval.visit(tree);
    }

    public static OnDemandQuery parseOnDemandQuery(String onDemandQueryString) throws SiddhiParserException {

        CharStream input = CharStreams.fromString(onDemandQueryString);
        SiddhiQLLexer lexer = new SiddhiQLLexer(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(SiddhiErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SiddhiQLParser parser = new SiddhiQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(SiddhiErrorListener.INSTANCE);
        ParseTree tree = parser.store_query_final();

        SiddhiQLVisitor eval = new SiddhiQLBaseVisitorImpl();
        OnDemandQuery onDemandQuery = (OnDemandQuery) eval.visit(tree);
        return onDemandQuery;
    }

    public static StoreQuery parseStoreQuery(String storeQuery) throws SiddhiParserException {

        OnDemandQuery onDemandQuery = parseOnDemandQuery(storeQuery);
        return new StoreQuery(onDemandQuery);
    }

    public static Expression parseExpression(String expression) {

        CharStream input = CharStreams.fromString(expression);
        SiddhiQLLexer lexer = new SiddhiQLLexer(input);
        lexer.removeErrorListeners();
        lexer.addErrorListener(SiddhiErrorListener.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SiddhiQLParser parser = new SiddhiQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(SiddhiErrorListener.INSTANCE);
        ParseTree tree = parser.expression();

        SiddhiQLVisitor eval = new SiddhiQLBaseVisitorImpl();
        return (Expression) eval.visit(tree);
    }

    public static String updateVariables(String siddhiApp) {

        String updatedSiddhiApp = siddhiApp;
        if (siddhiApp.contains("$")) {
            Pattern variablePattern = Pattern.compile("\\$\\{(\\w+)\\}");
            Matcher variableMatcher = variablePattern.matcher(siddhiApp);
            while (variableMatcher.find()) {
                String key = variableMatcher.group(1);
                String value = System.getProperty(key);
                if (value == null) {
                    value = System.getenv(key);
                    if (value == null) {
                        int startIndex = variableMatcher.start(0);
                        int endIndex = variableMatcher.end(0);
                        String upToStartIndex = siddhiApp.substring(0, startIndex);
                        int lineStartIndex = upToStartIndex.lastIndexOf("\n") + 1;
                        int lineNumber = upToStartIndex.split("\n").length;
                        Pattern appNamePattern = Pattern.compile("@app:name\\(\\W*('|\")(\\w+)('|\")\\W*\\)");
                        Matcher appNameMatcher = appNamePattern.matcher(siddhiApp);
                        if (appNameMatcher.find()) {
                            String appName = appNameMatcher.group(2);
                            throw new SiddhiParserException("No system or environmental variable found for '${"
                                    + key + "}', for Siddhi App '" + appName + "'",
                                    new int[]{lineNumber, startIndex - lineStartIndex},
                                    new int[]{lineNumber, endIndex - lineStartIndex});
                        } else {
                            throw new SiddhiParserException("No system or environmental variable found for '${"
                                    + key + "}'", new int[]{lineNumber, startIndex - lineStartIndex},
                                    new int[]{lineNumber, endIndex - lineStartIndex});
                        }
                    }
                }
                updatedSiddhiApp = updatedSiddhiApp.replaceAll("\\$\\{(" + key + ")\\}", value);
            }
        }
        return updatedSiddhiApp;
    }
}
