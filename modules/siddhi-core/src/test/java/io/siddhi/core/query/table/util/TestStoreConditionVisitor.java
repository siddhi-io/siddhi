/*
 *  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.core.query.table.util;

import io.siddhi.core.exception.OperationNotSupportedException;
import io.siddhi.core.table.record.BaseExpressionVisitor;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.expression.condition.Compare;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Class which is used by the Siddhi runtime for instructions on converting the SiddhiQL condition to the condition
 * format understood by the underlying RDBMS data store.
 */
public class TestStoreConditionVisitor extends BaseExpressionVisitor {

    private StringBuilder condition;
    private String finalCompiledCondition;
    private String tableName;

    private Map<String, Object> placeholders;
    private SortedMap<Integer, Object> parameters;

    private int streamVarCount;
    private int constantCount;

    private boolean isContainsConditionExist;
    private boolean nextProcessContainsPattern;
    private int ordinalOfContainPattern = 1;

    private String[] supportedFunctions = {"sum", "avg", "min", "max"};

    public TestStoreConditionVisitor(String tableName) {
        this.tableName = tableName;
        this.condition = new StringBuilder();
        this.streamVarCount = 0;
        this.constantCount = 0;
        this.placeholders = new HashMap<>();
        this.parameters = new TreeMap<>();
    }

    private TestStoreConditionVisitor() {
        //preventing initialization
    }

    public static boolean isEmpty(String field) {
        return (field == null || field.trim().length() == 0);
    }

    public String returnCondition() {
        this.parametrizeCondition();
        return this.finalCompiledCondition.trim();
    }

    public int getOrdinalOfContainPattern() {
        return ordinalOfContainPattern;
    }

    public SortedMap<Integer, Object> getParameters() {
        return this.parameters;
    }

    public boolean isContainsConditionExist() {
        return isContainsConditionExist;
    }

    public int getStreamVarCount() {
        return streamVarCount;
    }

    @Override
    public void beginVisitAnd() {
        condition.append(TestStoreTableConstants.OPEN_PARENTHESIS);
    }

    @Override
    public void endVisitAnd() {
        condition.append(TestStoreTableConstants.CLOSE_PARENTHESIS);
    }

    @Override
    public void beginVisitAndLeftOperand() {
        //Not applicable
    }

    @Override
    public void endVisitAndLeftOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitAndRightOperand() {
        condition.append(TestStoreTableConstants.SQL_AND).append(TestStoreTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitAndRightOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitOr() {
        condition.append(TestStoreTableConstants.OPEN_PARENTHESIS);
    }

    @Override
    public void endVisitOr() {
        condition.append(TestStoreTableConstants.CLOSE_PARENTHESIS);
    }

    @Override
    public void beginVisitOrLeftOperand() {
        //Not applicable
    }

    @Override
    public void endVisitOrLeftOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitOrRightOperand() {
        condition.append(TestStoreTableConstants.SQL_OR).append(TestStoreTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitOrRightOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitNot() {
        condition.append(TestStoreTableConstants.SQL_NOT).append(TestStoreTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitNot() {
        //Not applicable
    }

    @Override
    public void beginVisitCompare(Compare.Operator operator) {
        condition.append(TestStoreTableConstants.OPEN_PARENTHESIS);
    }

    @Override
    public void endVisitCompare(Compare.Operator operator) {
        condition.append(TestStoreTableConstants.CLOSE_PARENTHESIS);
    }

    @Override
    public void beginVisitCompareLeftOperand(Compare.Operator operator) {
        //Not applicable
    }

    @Override
    public void endVisitCompareLeftOperand(Compare.Operator operator) {
        //Not applicable
    }

    @Override
    public void beginVisitCompareRightOperand(Compare.Operator operator) {
        switch (operator) {
            case EQUAL:
                condition.append(TestStoreTableConstants.SQL_COMPARE_EQUAL);
                break;
            case GREATER_THAN:
                condition.append(TestStoreTableConstants.SQL_COMPARE_GREATER_THAN);
                break;
            case GREATER_THAN_EQUAL:
                condition.append(TestStoreTableConstants.SQL_COMPARE_GREATER_THAN_EQUAL);
                break;
            case LESS_THAN:
                condition.append(TestStoreTableConstants.SQL_COMPARE_LESS_THAN);
                break;
            case LESS_THAN_EQUAL:
                condition.append(TestStoreTableConstants.SQL_COMPARE_LESS_THAN_EQUAL);
                break;
            case NOT_EQUAL:
                condition.append(TestStoreTableConstants.SQL_COMPARE_NOT_EQUAL);
                break;
        }
        condition.append(TestStoreTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitCompareRightOperand(Compare.Operator operator) {
        //Not applicable
    }

    @Override
    public void beginVisitIsNull(String streamId) {
    }

    @Override
    public void endVisitIsNull(String streamId) {
        condition.append(TestStoreTableConstants.SQL_IS_NULL).append(TestStoreTableConstants.WHITESPACE);
    }

    @Override
    public void beginVisitIn(String storeId) {
        condition.append(TestStoreTableConstants.SQL_IN).append(TestStoreTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitIn(String storeId) {
        //Not applicable
    }

    @Override
    public void beginVisitConstant(Object value, Attribute.Type type) {
        String name;
        if (nextProcessContainsPattern) {
            name = this.generatePatternConstantName();
            nextProcessContainsPattern = false;
        } else {
            name = this.generateConstantName();
        }
        this.placeholders.put(name, new Constant(value, type));
        condition.append("[").append(name).append("]").append(TestStoreTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitConstant(Object value, Attribute.Type type) {
        //Not applicable
    }

    @Override
    public void beginVisitMath(MathOperator mathOperator) {
        condition.append(TestStoreTableConstants.OPEN_PARENTHESIS);
    }

    @Override
    public void endVisitMath(MathOperator mathOperator) {
        condition.append(TestStoreTableConstants.CLOSE_PARENTHESIS);
    }

    @Override
    public void beginVisitMathLeftOperand(MathOperator mathOperator) {
        //Not applicable
    }

    @Override
    public void endVisitMathLeftOperand(MathOperator mathOperator) {
        //Not applicable
    }

    @Override
    public void beginVisitMathRightOperand(MathOperator mathOperator) {
        switch (mathOperator) {
            case ADD:
                condition.append(TestStoreTableConstants.SQL_MATH_ADD);
                break;
            case DIVIDE:
                condition.append(TestStoreTableConstants.SQL_MATH_DIVIDE);
                break;
            case MOD:
                condition.append(TestStoreTableConstants.SQL_MATH_MOD);
                break;
            case MULTIPLY:
                condition.append(TestStoreTableConstants.SQL_MATH_MULTIPLY);
                break;
            case SUBTRACT:
                condition.append(TestStoreTableConstants.SQL_MATH_SUBTRACT);
                break;
        }
        condition.append(TestStoreTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitMathRightOperand(MathOperator mathOperator) {
        //Not applicable
    }

    @Override
    public void beginVisitAttributeFunction(String namespace, String functionName) {
        if (isEmpty(namespace) &&
                (Arrays.stream(supportedFunctions).anyMatch(functionName::equals))) {
            condition.append(functionName).append(TestStoreTableConstants.OPEN_PARENTHESIS);
        } else if (namespace.trim().equals("str") && functionName.equals("contains")) {
            condition.append("CONTAINS").append(TestStoreTableConstants.OPEN_PARENTHESIS);
            isContainsConditionExist = true;
            nextProcessContainsPattern = true;
        } else {
            throw new OperationNotSupportedException("The RDBMS Event table does not support functions other than " +
                    "sum(), avg(), min(), max() and str:contains() but function '" +
                    ((isEmpty(namespace)) ? "" + functionName : namespace + ":" + functionName) +
                    "' was specified.");

        }
    }

    @Override
    public void endVisitAttributeFunction(String namespace, String functionName) {
        if ((namespace.trim().equals("str") && functionName.equals("contains")) ||
                (Arrays.stream(supportedFunctions).anyMatch(functionName::equals))) {
            condition.append(TestStoreTableConstants.CLOSE_PARENTHESIS).append(TestStoreTableConstants.WHITESPACE);
        } else {
            throw new OperationNotSupportedException("The RDBMS Event table does not support functions other than " +
                    "sum(), avg(), min(), max() and str:contains() but function '" +
                    ((isEmpty(namespace)) ? "" + functionName : namespace + ":" + functionName) +
                    "' was specified.");
        }
    }

    @Override
    public void beginVisitParameterAttributeFunction(int index) {
        //Not applicable
    }

    @Override
    public void endVisitParameterAttributeFunction(int index) {
        //Not applicable
    }

    @Override
    public void beginVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
        String name;
        if (nextProcessContainsPattern) {
            name = this.generatePatternStreamVarName();
            nextProcessContainsPattern = false;
        } else {
            name = this.generateStreamVarName();
        }
        this.placeholders.put(name, new Attribute(id, type));
        condition.append("[").append(name).append("]").append(TestStoreTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
        //Not applicable
    }

    @Override
    public void beginVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
        condition.append(this.tableName).append(".").append(attributeName).append(TestStoreTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
        //Not applicable
    }

    /**
     * Util method for walking through the generated condition string and isolating the parameters which will be filled
     * in later as part of building the SQL statement. This method will:
     * (a) eliminate all temporary placeholders and put "?" in their places.
     * (b) build and maintain a sorted map of ordinals and the coresponding parameters which will fit into the above
     * places in the PreparedStatement.
     */
    private void parametrizeCondition() {
        String query = this.condition.toString();
        String[] tokens = query.split("\\[");
        int ordinal = 1;
        for (String token : tokens) {
            if (token.contains("]")) {
                String candidate = token.substring(0, token.indexOf("]"));
                if (this.placeholders.containsKey(candidate)) {
                    this.parameters.put(ordinal, this.placeholders.get(candidate));
                    ordinal++;
                    if (candidate.equals("pattern-value")) {
                        ordinalOfContainPattern = ordinal;
                    }
                }
            }
        }
        for (String placeholder : this.placeholders.keySet()) {
            query = query.replace("[" + placeholder + "]", "?");
        }
        this.finalCompiledCondition = query;
    }

    /**
     * Method for generating a temporary placeholder for stream variables.
     *
     * @return a placeholder string of known format.
     */
    private String generateStreamVarName() {
        String name = "strVar" + this.streamVarCount;
        this.streamVarCount++;
        return name;
    }

    /**
     * Method for generating a temporary placeholder for constants.
     *
     * @return a placeholder string of known format.
     */
    private String generateConstantName() {
        String name = "const" + this.constantCount;
        this.constantCount++;
        return name;
    }

    /**
     * Method for generating a temporary placeholder for contains pattern as stream variables.
     *
     * @return a placeholder string of known format.
     */
    private String generatePatternStreamVarName() {
        String name = "pattern-value" + this.streamVarCount;
        this.streamVarCount++;
        return name;
    }

    /**
     * Method for generating a temporary placeholder for contains pattern as constants.
     *
     * @return a placeholder string of known format.
     */
    private String generatePatternConstantName() {
        String name = "pattern-value" + this.constantCount;
        this.constantCount++;
        return name;
    }

}
