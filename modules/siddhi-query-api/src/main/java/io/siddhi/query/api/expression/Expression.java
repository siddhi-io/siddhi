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
package io.siddhi.query.api.expression;

import io.siddhi.query.api.SiddhiElement;
import io.siddhi.query.api.aggregation.TimePeriod;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import io.siddhi.query.api.expression.condition.And;
import io.siddhi.query.api.expression.condition.Compare;
import io.siddhi.query.api.expression.condition.In;
import io.siddhi.query.api.expression.condition.IsNull;
import io.siddhi.query.api.expression.condition.Not;
import io.siddhi.query.api.expression.condition.Or;
import io.siddhi.query.api.expression.constant.BoolConstant;
import io.siddhi.query.api.expression.constant.DoubleConstant;
import io.siddhi.query.api.expression.constant.FloatConstant;
import io.siddhi.query.api.expression.constant.IntConstant;
import io.siddhi.query.api.expression.constant.LongConstant;
import io.siddhi.query.api.expression.constant.StringConstant;
import io.siddhi.query.api.expression.constant.TimeConstant;
import io.siddhi.query.api.expression.math.Add;
import io.siddhi.query.api.expression.math.Divide;
import io.siddhi.query.api.expression.math.Mod;
import io.siddhi.query.api.expression.math.Multiply;
import io.siddhi.query.api.expression.math.Subtract;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Siddhi expression
 */
public abstract class Expression implements SiddhiElement {

    private static final long serialVersionUID = 1L;
    private int[] queryContextStartIndex;
    private int[] queryContextEndIndex;

    public static StringConstant value(String value) {

        return new StringConstant(value);
    }

    public static IntConstant value(int value) {

        return new IntConstant(value);
    }

    public static LongConstant value(long value) {

        return new LongConstant(value);
    }

    public static DoubleConstant value(double value) {

        return new DoubleConstant(value);
    }

    public static FloatConstant value(float value) {

        return new FloatConstant(value);
    }

    public static BoolConstant value(boolean value) {

        return new BoolConstant(value);
    }

    public static Variable variable(String attributeName) {

        return new Variable(attributeName);
    }

    public static Add add(Expression leftValue, Expression rightValue) {

        return new Add(leftValue, rightValue);
    }

    public static Subtract subtract(Expression leftValue, Expression rightValue) {

        return new Subtract(leftValue, rightValue);
    }

    public static Multiply multiply(Expression leftValue, Expression rightValue) {

        return new Multiply(leftValue, rightValue);
    }

    public static Divide divide(Expression leftValue, Expression rightValue) {

        return new Divide(leftValue, rightValue);
    }

    public static Mod mod(Expression leftValue, Expression rightValue) {

        return new Mod(leftValue, rightValue);
    }

    public static Expression function(String extensionNamespace, String extensionFunctionName,
                                      Expression... expressions) {

        return new AttributeFunction(extensionNamespace, extensionFunctionName, expressions);
    }

    public static Expression function(String functionName, Expression... expressions) {

        return new AttributeFunction("", functionName, expressions);
    }

    public static Expression compare(Expression leftExpression, Compare.Operator operator,
                                     Expression rightExpression) {

        return new Compare(leftExpression, operator, rightExpression);
    }

    public static Expression in(Expression leftExpression, String streamId) {

        return new In(leftExpression, streamId);
    }

    public static Expression and(Expression leftExpression, Expression rightExpression) {

        return new And(leftExpression, rightExpression);
    }

    public static Expression or(Expression leftExpression, Expression rightExpression) {

        return new Or(leftExpression, rightExpression);
    }

    public static Expression not(Expression expression) {

        return new Not(expression);
    }

    public static Expression isNull(Expression expression) {

        return new IsNull(expression);
    }

    public static Expression isNullStream(String streamId) {

        return new IsNull(streamId, null, false);
    }

    public static Expression isNullStream(String streamId, int streamIndex) {

        return new IsNull(streamId, streamIndex, false);
    }

    public static Expression isNullFaultStream(String streamId) {

        return new IsNull(streamId, null, false, true);
    }

    public static Expression isNullFaultStream(String streamId, int streamIndex) {

        return new IsNull(streamId, streamIndex, false, true);
    }

    public static Expression isNullInnerStream(String streamId) {

        return new IsNull(streamId, null, true);
    }

    public static Expression isNullInnerStream(String streamId, int streamIndex) {

        return new IsNull(streamId, streamIndex, true);
    }

    @Override
    public int[] getQueryContextStartIndex() {

        return queryContextStartIndex;
    }

    @Override
    public void setQueryContextStartIndex(int[] lineAndColumn) {

        queryContextStartIndex = lineAndColumn;
    }

    @Override
    public int[] getQueryContextEndIndex() {

        return queryContextEndIndex;
    }

    @Override
    public void setQueryContextEndIndex(int[] lineAndColumn) {

        queryContextEndIndex = lineAndColumn;
    }

    /**
     * Time constant factory class
     */
    public abstract static class Time {

        public static TimeConstant milliSec(long i) {

            return new TimeConstant((long) i);
        }

        public static TimeConstant milliSec(int i) {

            return milliSec((long) i);
        }

        public static TimeConstant sec(long i) {

            return new TimeConstant(((long) i) * 1000);
        }

        public static TimeConstant sec(int i) {

            return sec((long) i);
        }

        public static TimeConstant minute(long i) {

            return new TimeConstant(((long) i) * 60 * 1000);
        }

        public static TimeConstant minute(int i) {

            return minute((long) i);
        }

        public static TimeConstant hour(long i) {

            return new TimeConstant(((long) i) * 60 * 60 * 1000);
        }

        public static TimeConstant hour(int i) {

            return hour((long) i);
        }

        public static TimeConstant day(long i) {

            return new TimeConstant(((long) i) * 24 * 60 * 60 * 1000);
        }

        public static TimeConstant day(int i) {

            return day((long) i);
        }

        public static TimeConstant week(long i) {

            return new TimeConstant(((long) i) * 7 * 24 * 60 * 60 * 1000);
        }

        public static TimeConstant week(int i) {

            return week((long) i);
        }

        public static TimeConstant month(long i) {

            return new TimeConstant(((long) i) * 30 * 24 * 60 * 60 * 1000);
        }

        public static TimeConstant month(int i) {

            return month((long) i);
        }

        public static TimeConstant year(long i) {

            return new TimeConstant(((long) i) * 365 * 24 * 60 * 60 * 1000);
        }

        public static TimeConstant year(int i) {

            return year((long) i);
        }

        public static Long timeToLong(String value) {

            Pattern timeValuePattern = Pattern.compile("\\d+");
            Pattern durationPattern = Pattern.compile("\\D+");
            Matcher timeMatcher = timeValuePattern.matcher(value);
            Matcher durationMatcher = durationPattern.matcher(value);
            int timeValue;
            TimePeriod.Duration duration;
            if (timeMatcher.find() && durationMatcher.find()) {
                duration = normalizeDuration(durationMatcher.group(0).trim());
                timeValue = Integer.parseInt(timeMatcher.group(0));
                switch (duration) {
                    case SECONDS:
                        return Expression.Time.sec(timeValue).value();
                    case MINUTES:
                        return Expression.Time.minute(timeValue).value();
                    case HOURS:
                        return Expression.Time.hour(timeValue).value();
                    case DAYS:
                        return Expression.Time.day(timeValue).value();
                    case YEARS:
                        return Expression.Time.year(timeValue).value();
                    default:
                        return Expression.Time.month(timeValue).value();
                }
            } else {
                throw new SiddhiAppValidationException("Provided retention value cannot be identified. retention " +
                        "period: " + value + ".");
            }
        }

        public static TimePeriod.Duration normalizeDuration(String value) {

            switch (value.toLowerCase()) {
                case "sec":
                case "seconds":
                case "second":
                    return TimePeriod.Duration.SECONDS;
                case "min":
                case "minutes":
                case "minute":
                    return TimePeriod.Duration.MINUTES;
                case "h":
                case "hour":
                case "hours":
                    return TimePeriod.Duration.HOURS;
                case "days":
                case "day":
                    return TimePeriod.Duration.DAYS;
                case "month":
                case "months":
                    return TimePeriod.Duration.MONTHS;
                case "year":
                case "years":
                    return TimePeriod.Duration.YEARS;
                default:
                    throw new SiddhiAppValidationException("Duration '" + value + "' does not exists ");
            }
        }

    }
}
