/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.query.compiler.langserver;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.FailedPredicateException;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.misc.IntervalSet;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * {@code LanguageServerParserErrorStrategy} Handles exceptions thrown by parser strategically.
 */
public class LanguageServerParserErrorStrategy extends DefaultErrorStrategy {

    private Map<String, ParseTree> parseTreeMap;
    private Parser recognizer;

    @Override
    public void reportError(Parser recognizer, RecognitionException e) {
        this.recognizer = recognizer;
        generateContextTree();
        recognizer.notifyErrorListeners(e.getOffendingToken(), e.getMessage(), e);
    }

    @Override
    protected void reportNoViableAlternative(Parser recognizer, NoViableAltException e) {
        this.recognizer = recognizer;
        generateContextTree();
        TokenStream tokens = recognizer.getInputStream();
        String input;
        if (tokens != null) {
            if (e.getStartToken().getType() == -1) {
                input = "<EOF>";
            } else {
                input = tokens.getText(e.getStartToken(), e.getOffendingToken());
            }
        } else {
            input = "<unknown input>";
        }

        String msg = "No viable alternative at input " + this.escapeWSAndQuote(input);
        recognizer.notifyErrorListeners(e.getOffendingToken(), msg, e);
    }

    @Override
    protected void reportInputMismatch(Parser recognizer, InputMismatchException e) {
        this.recognizer = recognizer;
        generateContextTree();
        String msg = "Mismatched input " + this.getTokenErrorDisplay(e.getOffendingToken()) + " expecting " +
                e.getExpectedTokens().toString(recognizer.getVocabulary());
        recognizer.notifyErrorListeners(e.getOffendingToken(), msg, e);
    }

    @Override
    protected void reportFailedPredicate(Parser recognizer, FailedPredicateException e) {
        this.recognizer = recognizer;
        generateContextTree();
        String ruleName = recognizer.getRuleNames()[recognizer.getContext().getRuleIndex()];
        String msg = "Rule " + ruleName + " " + e.getMessage();
        recognizer.notifyErrorListeners(e.getOffendingToken(), msg, e);
    }

    @Override
    protected void reportUnwantedToken(Parser recognizer) {
        this.recognizer = recognizer;
        generateContextTree();
        if (!this.inErrorRecoveryMode(recognizer)) {
            this.beginErrorCondition(recognizer);
            Token token = recognizer.getCurrentToken();
            String tokenName = this.getTokenErrorDisplay(token);
            IntervalSet expecting = this.getExpectedTokens(recognizer);
            String msg =
                    "Extraneous input " + tokenName + " expecting " + expecting.toString(recognizer.getVocabulary());
            recognizer.notifyErrorListeners(token, msg, null);
        }
    }

    @Override
    protected void reportMissingToken(Parser recognizer) {
        this.recognizer = recognizer;
        generateContextTree();
        if (!this.inErrorRecoveryMode(recognizer)) {
            this.beginErrorCondition(recognizer);
            Token token = recognizer.getCurrentToken();
            IntervalSet expecting = this.getExpectedTokens(recognizer);
            String msg =
                    "Missing " + expecting.toString(recognizer.getVocabulary()) + " at " +
                            this.getTokenErrorDisplay(token);
            recognizer.notifyErrorListeners(token, msg, null);
        }
    }

    //todo: add this method in the case of goalPosition is placed after the erroneous position in the source code.
    // i.e : current context in which the cursor is present, can't be found as the SiddhiParser doesn't parse the source
    // content beyond the erroneous point and an ParseTree is not built.
//    public Boolean isCursorOnLeft(int[] cursorPosition, int[] errorPosition) {
//        if (cursorPosition[0] <= errorPosition[0]) {
//            if (cursorPosition[0] == errorPosition[0]) {
//                return (cursorPosition[1] <= errorPosition[1]);
//            } else {
//                return true;
//            }
//        }
//        return false;
//    }

    public void traverseUp(ParserRuleContext context) {
        if (context.parent != null) {
            parseTreeMap.put(context.parent.getClass().getName(), context.parent);
            traverseUp((ParserRuleContext) context.parent);
        }
    }

    public Map<String, ParseTree> getParseTreeMap() {
        return this.parseTreeMap;
    }

    /**
     * Generates an instance of the  {@link LSErrorNode} class which contains information of the erroneous token.
     *
     * @param recognizer
     * @return
     */
    public LSErrorNode generateErrorNode(Parser recognizer) {
        // {'','\n',' '} have token type 112
        CommonTokenStream inputStream = (CommonTokenStream) recognizer.getInputStream();
        List<Token> tokens = inputStream.getTokens();
        Token currentToken = recognizer.getCurrentToken();
        Predicate<Token> errorPredicate = (token) -> {
            if (token.getLine() == currentToken.getLine() &&
                    token.getCharPositionInLine() > currentToken.getCharPositionInLine() &&
                    LSErrorNode.UNWANTED_TOKEN_TYPE == token.getType()) {
                return true;
            }
            return false;
        };
        List<Token> filteredTokens = tokens.stream().filter(errorPredicate).
                collect(Collectors.toList());
        LSErrorNode errorNode = new LSErrorNode(this.recognizer.getContext(), currentToken.getText());
        if (!filteredTokens.isEmpty()) {
            StringBuilder errorClause = new StringBuilder();
            for (Token filteredToken : filteredTokens) {
                errorClause.append(filteredToken.getText());
            }
            errorNode.setErroneousSymbol(errorClause.toString());

        }
        return errorNode;
    }

    private void generateContextTree() {
        parseTreeMap = new LinkedHashMap<>();
        this.parseTreeMap.put(LSErrorNode.class.getName(), generateErrorNode(this.recognizer));
        this.parseTreeMap.put(this.recognizer.getContext().getClass().getName(), this.recognizer.getContext());
        traverseUp(this.recognizer.getContext());
    }
}
