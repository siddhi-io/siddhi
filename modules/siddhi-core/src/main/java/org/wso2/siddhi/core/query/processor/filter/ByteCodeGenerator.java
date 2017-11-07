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
package org.wso2.siddhi.core.query.processor.filter;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.executor.condition.AndConditionExpressionExecutor;
import org.wso2.siddhi.core.executor.condition.BoolConditionExpressionExecutor;
import org.wso2.siddhi.core.executor.condition.InConditionExpressionExecutor;
import org.wso2.siddhi.core.executor.condition.IsNullConditionExpressionExecutor;
import org.wso2.siddhi.core.executor.condition.IsNullStreamConditionExpressionExecutor;
import org.wso2.siddhi.core.executor.condition.NotConditionExpressionExecutor;
import org.wso2.siddhi.core.executor.condition.OrConditionExpressionExecutor;
import org.wso2.siddhi.core.executor.condition.compare.equal.EqualCompareConditionExpressionExecutorBoolBool;
import org.wso2.siddhi.core.executor.condition.compare.equal.EqualCompareConditionExpressionExecutorDoubleDouble;
import org.wso2.siddhi.core.executor.condition.compare.equal.EqualCompareConditionExpressionExecutorDoubleFloat;
import org.wso2.siddhi.core.executor.condition.compare.equal.EqualCompareConditionExpressionExecutorDoubleInt;
import org.wso2.siddhi.core.executor.condition.compare.equal.EqualCompareConditionExpressionExecutorDoubleLong;
import org.wso2.siddhi.core.executor.condition.compare.equal.EqualCompareConditionExpressionExecutorFloatDouble;
import org.wso2.siddhi.core.executor.condition.compare.equal.EqualCompareConditionExpressionExecutorFloatFloat;
import org.wso2.siddhi.core.executor.condition.compare.equal.EqualCompareConditionExpressionExecutorFloatInt;
import org.wso2.siddhi.core.executor.condition.compare.equal.EqualCompareConditionExpressionExecutorFloatLong;
import org.wso2.siddhi.core.executor.condition.compare.equal.EqualCompareConditionExpressionExecutorIntDouble;
import org.wso2.siddhi.core.executor.condition.compare.equal.EqualCompareConditionExpressionExecutorIntFloat;
import org.wso2.siddhi.core.executor.condition.compare.equal.EqualCompareConditionExpressionExecutorIntInt;
import org.wso2.siddhi.core.executor.condition.compare.equal.EqualCompareConditionExpressionExecutorIntLong;
import org.wso2.siddhi.core.executor.condition.compare.equal.EqualCompareConditionExpressionExecutorLongDouble;
import org.wso2.siddhi.core.executor.condition.compare.equal.EqualCompareConditionExpressionExecutorLongFloat;
import org.wso2.siddhi.core.executor.condition.compare.equal.EqualCompareConditionExpressionExecutorLongInt;
import org.wso2.siddhi.core.executor.condition.compare.equal.EqualCompareConditionExpressionExecutorLongLong;
import org.wso2.siddhi.core.executor.condition.compare.equal.EqualCompareConditionExpressionExecutorStringString;
import org.wso2.siddhi.core.executor.condition.compare.
        greaterthan.GreaterThanCompareConditionExpressionExecutorDoubleDouble;
import org.wso2.siddhi.core.executor.condition.compare.
        greaterthan.GreaterThanCompareConditionExpressionExecutorDoubleFloat;
import org.wso2.siddhi.core.executor.condition.compare.
        greaterthan.GreaterThanCompareConditionExpressionExecutorDoubleInt;
import org.wso2.siddhi.core.executor.condition.compare.
        greaterthan.GreaterThanCompareConditionExpressionExecutorDoubleLong;
import org.wso2.siddhi.core.executor.condition.compare.
        greaterthan.GreaterThanCompareConditionExpressionExecutorFloatDouble;
import org.wso2.siddhi.core.executor.condition.compare.
        greaterthan.GreaterThanCompareConditionExpressionExecutorFloatFloat;
import org.wso2.siddhi.core.executor.condition.compare.
        greaterthan.GreaterThanCompareConditionExpressionExecutorFloatInt;
import org.wso2.siddhi.core.executor.condition.compare.
        greaterthan.GreaterThanCompareConditionExpressionExecutorFloatLong;
import org.wso2.siddhi.core.executor.condition.compare.
        greaterthan.GreaterThanCompareConditionExpressionExecutorIntDouble;
import org.wso2.siddhi.core.executor.condition.compare.
        greaterthan.GreaterThanCompareConditionExpressionExecutorIntFloat;
import org.wso2.siddhi.core.executor.condition.compare.
        greaterthan.GreaterThanCompareConditionExpressionExecutorIntInt;
import org.wso2.siddhi.core.executor.condition.compare.
        greaterthan.GreaterThanCompareConditionExpressionExecutorIntLong;
import org.wso2.siddhi.core.executor.condition.compare.
        greaterthan.GreaterThanCompareConditionExpressionExecutorLongDouble;
import org.wso2.siddhi.core.executor.condition.compare.
        greaterthan.GreaterThanCompareConditionExpressionExecutorLongFloat;
import org.wso2.siddhi.core.executor.condition.compare.
        greaterthan.GreaterThanCompareConditionExpressionExecutorLongInt;
import org.wso2.siddhi.core.executor.condition.compare.
        greaterthan.GreaterThanCompareConditionExpressionExecutorLongLong;
import org.wso2.siddhi.core.executor.condition.compare.greaterthanequal.
        GreaterThanEqualCompareConditionExpressionExecutorDoubleDouble;
import org.wso2.siddhi.core.executor.condition.compare.greaterthanequal.
        GreaterThanEqualCompareConditionExpressionExecutorDoubleFloat;
import org.wso2.siddhi.core.executor.condition.compare.greaterthanequal.
        GreaterThanEqualCompareConditionExpressionExecutorDoubleInt;
import org.wso2.siddhi.core.executor.condition.compare.greaterthanequal.
        GreaterThanEqualCompareConditionExpressionExecutorDoubleLong;
import org.wso2.siddhi.core.executor.condition.compare.greaterthanequal.
        GreaterThanEqualCompareConditionExpressionExecutorFloatDouble;
import org.wso2.siddhi.core.executor.condition.compare.greaterthanequal.
        GreaterThanEqualCompareConditionExpressionExecutorFloatFloat;
import org.wso2.siddhi.core.executor.condition.compare.greaterthanequal.
        GreaterThanEqualCompareConditionExpressionExecutorFloatInt;
import org.wso2.siddhi.core.executor.condition.compare.greaterthanequal.
        GreaterThanEqualCompareConditionExpressionExecutorFloatLong;
import org.wso2.siddhi.core.executor.condition.compare.greaterthanequal.
        GreaterThanEqualCompareConditionExpressionExecutorIntDouble;
import org.wso2.siddhi.core.executor.condition.compare.greaterthanequal.
        GreaterThanEqualCompareConditionExpressionExecutorIntFloat;
import org.wso2.siddhi.core.executor.condition.compare.greaterthanequal.
        GreaterThanEqualCompareConditionExpressionExecutorIntInt;
import org.wso2.siddhi.core.executor.condition.compare.greaterthanequal.
        GreaterThanEqualCompareConditionExpressionExecutorIntLong;
import org.wso2.siddhi.core.executor.condition.compare.greaterthanequal.
        GreaterThanEqualCompareConditionExpressionExecutorLongDouble;
import org.wso2.siddhi.core.executor.condition.compare.greaterthanequal.
        GreaterThanEqualCompareConditionExpressionExecutorLongFloat;
import org.wso2.siddhi.core.executor.condition.compare.greaterthanequal.
        GreaterThanEqualCompareConditionExpressionExecutorLongInt;
import org.wso2.siddhi.core.executor.condition.compare.greaterthanequal.
        GreaterThanEqualCompareConditionExpressionExecutorLongLong;
import org.wso2.siddhi.core.executor.condition.compare.lessthan.LessThanCompareConditionExpressionExecutorDoubleDouble;
import org.wso2.siddhi.core.executor.condition.compare.lessthan.LessThanCompareConditionExpressionExecutorDoubleFloat;
import org.wso2.siddhi.core.executor.condition.compare.lessthan.LessThanCompareConditionExpressionExecutorDoubleInt;
import org.wso2.siddhi.core.executor.condition.compare.lessthan.LessThanCompareConditionExpressionExecutorDoubleLong;
import org.wso2.siddhi.core.executor.condition.compare.lessthan.LessThanCompareConditionExpressionExecutorFloatDouble;
import org.wso2.siddhi.core.executor.condition.compare.lessthan.LessThanCompareConditionExpressionExecutorFloatFloat;
import org.wso2.siddhi.core.executor.condition.compare.lessthan.LessThanCompareConditionExpressionExecutorFloatInt;
import org.wso2.siddhi.core.executor.condition.compare.lessthan.LessThanCompareConditionExpressionExecutorFloatLong;
import org.wso2.siddhi.core.executor.condition.compare.lessthan.LessThanCompareConditionExpressionExecutorIntDouble;
import org.wso2.siddhi.core.executor.condition.compare.lessthan.LessThanCompareConditionExpressionExecutorIntFloat;
import org.wso2.siddhi.core.executor.condition.compare.lessthan.LessThanCompareConditionExpressionExecutorIntInt;
import org.wso2.siddhi.core.executor.condition.compare.lessthan.LessThanCompareConditionExpressionExecutorIntLong;
import org.wso2.siddhi.core.executor.condition.compare.lessthan.LessThanCompareConditionExpressionExecutorLongDouble;
import org.wso2.siddhi.core.executor.condition.compare.lessthan.LessThanCompareConditionExpressionExecutorLongFloat;
import org.wso2.siddhi.core.executor.condition.compare.lessthan.LessThanCompareConditionExpressionExecutorLongInt;
import org.wso2.siddhi.core.executor.condition.compare.lessthan.LessThanCompareConditionExpressionExecutorLongLong;
import org.wso2.siddhi.core.executor.condition.compare.lessthanequal.
        LessThanEqualCompareConditionExpressionExecutorDoubleDouble;
import org.wso2.siddhi.core.executor.condition.compare.lessthanequal.
        LessThanEqualCompareConditionExpressionExecutorDoubleFloat;
import org.wso2.siddhi.core.executor.condition.compare.lessthanequal.
        LessThanEqualCompareConditionExpressionExecutorDoubleInt;
import org.wso2.siddhi.core.executor.condition.compare.lessthanequal.
        LessThanEqualCompareConditionExpressionExecutorDoubleLong;
import org.wso2.siddhi.core.executor.condition.compare.lessthanequal.
        LessThanEqualCompareConditionExpressionExecutorFloatDouble;
import org.wso2.siddhi.core.executor.condition.compare.lessthanequal.
        LessThanEqualCompareConditionExpressionExecutorFloatFloat;
import org.wso2.siddhi.core.executor.condition.compare.lessthanequal.
        LessThanEqualCompareConditionExpressionExecutorFloatInt;
import org.wso2.siddhi.core.executor.condition.compare.lessthanequal
        .LessThanEqualCompareConditionExpressionExecutorFloatLong;
import org.wso2.siddhi.core.executor.condition.compare.lessthanequal.
        LessThanEqualCompareConditionExpressionExecutorIntDouble;
import org.wso2.siddhi.core.executor.condition.compare.lessthanequal.
        LessThanEqualCompareConditionExpressionExecutorIntFloat;
import org.wso2.siddhi.core.executor.condition.compare.lessthanequal.
        LessThanEqualCompareConditionExpressionExecutorIntInt;
import org.wso2.siddhi.core.executor.condition.compare.lessthanequal.
        LessThanEqualCompareConditionExpressionExecutorIntLong;
import org.wso2.siddhi.core.executor.condition.compare.lessthanequal.
        LessThanEqualCompareConditionExpressionExecutorLongDouble;
import org.wso2.siddhi.core.executor.condition.compare.lessthanequal.
        LessThanEqualCompareConditionExpressionExecutorLongFloat;
import org.wso2.siddhi.core.executor.condition.compare.lessthanequal.
        LessThanEqualCompareConditionExpressionExecutorLongInt;
import org.wso2.siddhi.core.executor.condition.compare.lessthanequal.
        LessThanEqualCompareConditionExpressionExecutorLongLong;
import org.wso2.siddhi.core.executor.condition.compare.notequal.NotEqualCompareConditionExpressionExecutorBoolBool;
import org.wso2.siddhi.core.executor.condition.compare.notequal.NotEqualCompareConditionExpressionExecutorDoubleDouble;
import org.wso2.siddhi.core.executor.condition.compare.notequal.NotEqualCompareConditionExpressionExecutorDoubleFloat;
import org.wso2.siddhi.core.executor.condition.compare.notequal.NotEqualCompareConditionExpressionExecutorDoubleInt;
import org.wso2.siddhi.core.executor.condition.compare.notequal.NotEqualCompareConditionExpressionExecutorDoubleLong;
import org.wso2.siddhi.core.executor.condition.compare.notequal.NotEqualCompareConditionExpressionExecutorFloatDouble;
import org.wso2.siddhi.core.executor.condition.compare.notequal.NotEqualCompareConditionExpressionExecutorFloatFloat;
import org.wso2.siddhi.core.executor.condition.compare.notequal.NotEqualCompareConditionExpressionExecutorFloatInt;
import org.wso2.siddhi.core.executor.condition.compare.notequal.NotEqualCompareConditionExpressionExecutorFloatLong;
import org.wso2.siddhi.core.executor.condition.compare.notequal.NotEqualCompareConditionExpressionExecutorIntDouble;
import org.wso2.siddhi.core.executor.condition.compare.notequal.NotEqualCompareConditionExpressionExecutorIntFloat;
import org.wso2.siddhi.core.executor.condition.compare.notequal.NotEqualCompareConditionExpressionExecutorIntInt;
import org.wso2.siddhi.core.executor.condition.compare.notequal.NotEqualCompareConditionExpressionExecutorIntLong;
import org.wso2.siddhi.core.executor.condition.compare.notequal.NotEqualCompareConditionExpressionExecutorLongDouble;
import org.wso2.siddhi.core.executor.condition.compare.notequal.NotEqualCompareConditionExpressionExecutorLongFloat;
import org.wso2.siddhi.core.executor.condition.compare.notequal.NotEqualCompareConditionExpressionExecutorLongInt;
import org.wso2.siddhi.core.executor.condition.compare.notequal.NotEqualCompareConditionExpressionExecutorLongLong;
import org.wso2.siddhi.core.executor.condition.compare.notequal.NotEqualCompareConditionExpressionExecutorStringString;
import org.wso2.siddhi.core.executor.math.add.AddExpressionExecutorDouble;
import org.wso2.siddhi.core.executor.math.add.AddExpressionExecutorFloat;
import org.wso2.siddhi.core.executor.math.add.AddExpressionExecutorInt;
import org.wso2.siddhi.core.executor.math.add.AddExpressionExecutorLong;
import org.wso2.siddhi.core.executor.math.divide.DivideExpressionExecutorDouble;
import org.wso2.siddhi.core.executor.math.divide.DivideExpressionExecutorFloat;
import org.wso2.siddhi.core.executor.math.divide.DivideExpressionExecutorInt;
import org.wso2.siddhi.core.executor.math.divide.DivideExpressionExecutorLong;
import org.wso2.siddhi.core.executor.math.mod.ModExpressionExecutorDouble;
import org.wso2.siddhi.core.executor.math.mod.ModExpressionExecutorFloat;
import org.wso2.siddhi.core.executor.math.mod.ModExpressionExecutorInt;
import org.wso2.siddhi.core.executor.math.mod.ModExpressionExecutorLong;
import org.wso2.siddhi.core.executor.math.multiply.MultiplyExpressionExecutorDouble;
import org.wso2.siddhi.core.executor.math.multiply.MultiplyExpressionExecutorFloat;
import org.wso2.siddhi.core.executor.math.multiply.MultiplyExpressionExecutorInt;
import org.wso2.siddhi.core.executor.math.multiply.MultiplyExpressionExecutorLong;
import org.wso2.siddhi.core.executor.math.subtract.SubtractExpressionExecutorDouble;
import org.wso2.siddhi.core.executor.math.subtract.SubtractExpressionExecutorFloat;
import org.wso2.siddhi.core.executor.math.subtract.SubtractExpressionExecutorInt;
import org.wso2.siddhi.core.executor.math.subtract.SubtractExpressionExecutorLong;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * This is the class that generates byte code for ExpressionExecutor.
 */
public class ByteCodeGenerator {

    static Map<Class<? extends ExpressionExecutor>, ByteCodeEmitter> byteCodegenerators;

    static {
        byteCodegenerators = new HashMap<Class<? extends ExpressionExecutor>, ByteCodeEmitter>();
        ByteCodeRegistry byteCode = new ByteCodeRegistry();
        // Condition Executors.
        byteCodegenerators.put(AndConditionExpressionExecutor.class, byteCode.
                new PrivateANDExpressionExecutorBytecodeEmitter());
        byteCodegenerators.put(OrConditionExpressionExecutor.class, byteCode.
                new PrivateORExpressionExecutorBytecodeEmitter());
        byteCodegenerators.put(NotConditionExpressionExecutor.class, byteCode.
                new PrivateNOTExpressionExecutorBytecodeEmitter());
        byteCodegenerators.put(VariableExpressionExecutor.class, byteCode.
                new PrivateVariableExpressionExecutorBytecodeEmitter());
        byteCodegenerators.put(ConstantExpressionExecutor.class, byteCode.
                new PrivateConstantExpressionExecutorBytecodeEmitter());
        byteCodegenerators.put(IsNullConditionExpressionExecutor.class,
                byteCode.new PrivateIsNullExpressionExecutorBytecodeEmitter());
        byteCodegenerators.put(IsNullStreamConditionExpressionExecutor.class,
                byteCode.new PrivateIsNullStreamExpressionExecutorBytecodeEmitter());
        byteCodegenerators.put(BoolConditionExpressionExecutor.class,
                byteCode.new PrivateBoolExpressionExecutorBytecodeEmitter());
        byteCodegenerators.put(InConditionExpressionExecutor.class,
                byteCode.new PrivateInConditionExpressionExecutorBytecodeEmitter());
        // Greater Than Operator.
        byteCodegenerators.put(GreaterThanCompareConditionExpressionExecutorFloatFloat.class,
                byteCode.new PrivateGreaterThanCompareConditionExpressionExecutorFloatFloatBytecodeEmitter());
        byteCodegenerators.put(GreaterThanCompareConditionExpressionExecutorFloatDouble.class,
                byteCode.new PrivateGreaterThanCompareConditionExpressionExecutorFloatDoubleBytecodeEmitter());
        byteCodegenerators.put(GreaterThanCompareConditionExpressionExecutorDoubleDouble.class,
                byteCode.new PrivateGreaterThanCompareConditionExpressionExecutorDoubleDoubleBytecodeEmitter());
        byteCodegenerators.put(GreaterThanCompareConditionExpressionExecutorDoubleFloat.class,
                byteCode.new PrivateGreaterThanCompareConditionExpressionExecutorDoubleFloatBytecodeEmitter());
        byteCodegenerators.put(GreaterThanCompareConditionExpressionExecutorDoubleInt.class,
                byteCode.new PrivateGreaterThanCompareConditionExpressionExecutorDoubleIntegerBytecodeEmitter());
        byteCodegenerators.put(GreaterThanCompareConditionExpressionExecutorIntDouble.class,
                byteCode.new PrivateGreaterThanCompareConditionExpressionExecutorIntegerDoubleBytecodeEmitter());
        byteCodegenerators.put(GreaterThanCompareConditionExpressionExecutorIntFloat.class,
                byteCode.new PrivateGreaterThanCompareConditionExpressionExecutorIntegerFloatBytecodeEmitter());
        byteCodegenerators.put(GreaterThanCompareConditionExpressionExecutorFloatInt.class,
                byteCode.new PrivateGreaterThanCompareConditionExpressionExecutorFloatIntegerBytecodeEmitter());
        byteCodegenerators.put(GreaterThanCompareConditionExpressionExecutorIntInt.class,
                byteCode.new PrivateGreaterThanCompareConditionExpressionExecutorIntegerIntegerBytecodeEmitter());
        byteCodegenerators.put(GreaterThanCompareConditionExpressionExecutorDoubleLong.class,
                byteCode.new PrivateGreaterThanCompareConditionExpressionExecutorDoubleLongBytecodeEmitter());
        byteCodegenerators.put(GreaterThanCompareConditionExpressionExecutorLongDouble.class,
                byteCode.new PrivateGreaterThanCompareConditionExpressionExecutorLongDoubleBytecodeEmitter());
        byteCodegenerators.put(GreaterThanCompareConditionExpressionExecutorFloatLong.class,
                byteCode.new PrivateGreaterThanCompareConditionExpressionExecutorFloatLongBytecodeEmitter());
        byteCodegenerators.put(GreaterThanCompareConditionExpressionExecutorLongFloat.class,
                byteCode.new PrivateGreaterThanCompareConditionExpressionExecutorLongFloatBytecodeEmitter());
        byteCodegenerators.put(GreaterThanCompareConditionExpressionExecutorIntLong.class,
                byteCode.new PrivateGreaterThanCompareConditionExpressionExecutorIntegerLongBytecodeEmitter());
        byteCodegenerators.put(GreaterThanCompareConditionExpressionExecutorLongInt.class,
                byteCode.new PrivateGreaterThanCompareConditionExpressionExecutorLongIntegerBytecodeEmitter());
        byteCodegenerators.put(GreaterThanCompareConditionExpressionExecutorLongLong.class,
                byteCode.new PrivateGreaterThanCompareConditionExpressionExecutorLongLongBytecodeEmitter());
        // Less Than Operator.
        byteCodegenerators.put(LessThanCompareConditionExpressionExecutorFloatFloat.class,
                byteCode.new PrivateLessThanCompareConditionExpressionExecutorFloatFloatBytecodeEmitter());
        byteCodegenerators.put(LessThanCompareConditionExpressionExecutorFloatDouble.class,
                byteCode.new PrivateLessThanCompareConditionExpressionExecutorFloatDoubleBytecodeEmitter());
        byteCodegenerators.put(LessThanCompareConditionExpressionExecutorDoubleDouble.class,
                byteCode.new PrivateLessThanCompareConditionExpressionExecutorDoubleDoubleBytecodeEmitter());
        byteCodegenerators.put(LessThanCompareConditionExpressionExecutorDoubleFloat.class,
                byteCode.new PrivateLessThanCompareConditionExpressionExecutorDoubleFloatBytecodeEmitter());
        byteCodegenerators.put(LessThanCompareConditionExpressionExecutorDoubleInt.class,
                byteCode.new PrivateLessThanCompareConditionExpressionExecutorDoubleIntegerBytecodeEmitter());
        byteCodegenerators.put(LessThanCompareConditionExpressionExecutorIntDouble.class,
                byteCode.new PrivateLessThanCompareConditionExpressionExecutorIntegerDoubleBytecodeEmitter());
        byteCodegenerators.put(LessThanCompareConditionExpressionExecutorIntFloat.class,
                byteCode.new PrivateLessThanCompareConditionExpressionExecutorIntegerFloatBytecodeEmitter());
        byteCodegenerators.put(LessThanCompareConditionExpressionExecutorFloatInt.class,
                byteCode.new PrivateLessThanCompareConditionExpressionExecutorFloatIntegerBytecodeEmitter());
        byteCodegenerators.put(LessThanCompareConditionExpressionExecutorLongLong.class,
                byteCode.new PrivateLessThanCompareConditionExpressionExecutorLongLongBytecodeEmitter());
        byteCodegenerators.put(LessThanCompareConditionExpressionExecutorIntLong.class,
                byteCode.new PrivateLessThanCompareConditionExpressionExecutorIntegerLongBytecodeEmitter());
        byteCodegenerators.put(LessThanCompareConditionExpressionExecutorLongInt.class,
                byteCode.new PrivateLessThanCompareConditionExpressionExecutorLongIntegerBytecodeEmitter());
        byteCodegenerators.put(LessThanCompareConditionExpressionExecutorLongFloat.class,
                byteCode.new PrivateLessThanCompareConditionExpressionExecutorLongFloatBytecodeEmitter());
        byteCodegenerators.put(LessThanCompareConditionExpressionExecutorFloatLong.class,
                byteCode.new PrivateLessThanCompareConditionExpressionExecutorFloatLongBytecodeEmitter());
        byteCodegenerators.put(LessThanCompareConditionExpressionExecutorLongDouble.class,
                byteCode.new PrivateLessThanCompareConditionExpressionExecutorLongDoubleBytecodeEmitter());
        byteCodegenerators.put(LessThanCompareConditionExpressionExecutorDoubleLong.class,
                byteCode.new PrivateLessThanCompareConditionExpressionExecutorDoubleLongBytecodeEmitter());
        byteCodegenerators.put(LessThanCompareConditionExpressionExecutorIntInt.class,
                byteCode.new PrivateLessThanCompareConditionExpressionExecutorIntegerIntegerBytecodeEmitter());
        // Greater Than Equal Operator.
        byteCodegenerators.put(GreaterThanEqualCompareConditionExpressionExecutorDoubleDouble.class,
                byteCode.new PrivateGreaterThanEqualCompareConditionExpressionExecutorDoubleDoubleBytecodeEmitter());
        byteCodegenerators.put(GreaterThanEqualCompareConditionExpressionExecutorDoubleFloat.class,
                byteCode.new PrivateGreaterThanEqualCompareConditionExpressionExecutorDoubleFloatBytecodeEmitter());
        byteCodegenerators.put(GreaterThanEqualCompareConditionExpressionExecutorDoubleInt.class,
                byteCode.new PrivateGreaterThanEqualCompareConditionExpressionExecutorDoubleIntegerBytecodeEmitter());
        byteCodegenerators.put(GreaterThanEqualCompareConditionExpressionExecutorDoubleLong.class,
                byteCode.new PrivateGreaterThanEqualCompareConditionExpressionExecutorDoubleLongBytecodeEmitter());
        byteCodegenerators.put(GreaterThanEqualCompareConditionExpressionExecutorFloatDouble.class,
                byteCode.new PrivateGreaterThanEqualCompareConditionExpressionExecutorFloatDoubleBytecodeEmitter());
        byteCodegenerators.put(GreaterThanEqualCompareConditionExpressionExecutorFloatFloat.class,
                byteCode.new PrivateGreaterThanEqualCompareConditionExpressionExecutorFloatFloatBytecodeEmitter());
        byteCodegenerators.put(GreaterThanEqualCompareConditionExpressionExecutorFloatInt.class,
                byteCode.new PrivateGreaterThanEqualCompareConditionExpressionExecutorFloatIntegerBytecodeEmitter());
        byteCodegenerators.put(GreaterThanEqualCompareConditionExpressionExecutorFloatLong.class,
                byteCode.new PrivateGreaterThanEqualCompareConditionExpressionExecutorFloatLongBytecodeEmitter());
        byteCodegenerators.put(GreaterThanEqualCompareConditionExpressionExecutorIntDouble.class,
                byteCode.new PrivateGreaterThanEqualCompareConditionExpressionExecutorIntegerDoubleBytecodeEmitter());
        byteCodegenerators.put(GreaterThanEqualCompareConditionExpressionExecutorIntFloat.class,
                byteCode.new PrivateGreaterThanEqualCompareConditionExpressionExecutorIntegerFloatBytecodeEmitter());
        byteCodegenerators.put(GreaterThanEqualCompareConditionExpressionExecutorIntInt.class,
                byteCode.new PrivateGreaterThanEqualCompareConditionExpressionExecutorIntegerIntegerBytecodeEmitter());
        byteCodegenerators.put(GreaterThanEqualCompareConditionExpressionExecutorIntLong.class,
                byteCode.new PrivateGreaterThanEqualCompareConditionExpressionExecutorIntegerLongBytecodeEmitter());
        byteCodegenerators.put(GreaterThanEqualCompareConditionExpressionExecutorLongDouble.class,
                byteCode.new PrivateGreaterThanEqualCompareConditionExpressionExecutorLongDoubleBytecodeEmitter());
        byteCodegenerators.put(GreaterThanEqualCompareConditionExpressionExecutorLongFloat.class,
                byteCode.new PrivateGreaterThanEqualCompareConditionExpressionExecutorLongFloatBytecodeEmitter());
        byteCodegenerators.put(GreaterThanEqualCompareConditionExpressionExecutorLongInt.class,
                byteCode.new PrivateGreaterThanEqualCompareConditionExpressionExecutorLongIntegerBytecodeEmitter());
        byteCodegenerators.put(GreaterThanEqualCompareConditionExpressionExecutorLongLong.class,
                byteCode.new PrivateGreaterThanEqualCompareConditionExpressionExecutorLongLongBytecodeEmitter());
        //Less Than Equal Operator.
        byteCodegenerators.put(LessThanEqualCompareConditionExpressionExecutorDoubleDouble.class,
                byteCode.new PrivateLessThanEqualCompareConditionExpressionExecutorDoubleDoubleBytecodeEmitter());
        byteCodegenerators.put(LessThanEqualCompareConditionExpressionExecutorDoubleFloat.class,
                byteCode.new PrivateLessThanEqualCompareConditionExpressionExecutorDoubleFloatBytecodeEmitter());
        byteCodegenerators.put(LessThanEqualCompareConditionExpressionExecutorDoubleInt.class,
                byteCode.new PrivateLessThanEqualCompareConditionExpressionExecutorDoubleIntegerBytecodeEmitter());
        byteCodegenerators.put(LessThanEqualCompareConditionExpressionExecutorDoubleLong.class,
                byteCode.new PrivateLessThanEqualCompareConditionExpressionExecutorDoubleLongBytecodeEmitter());
        byteCodegenerators.put(LessThanEqualCompareConditionExpressionExecutorFloatDouble.class,
                byteCode.new PrivateLessThanEqualCompareConditionExpressionExecutorFloatDoubleBytecodeEmitter());
        byteCodegenerators.put(LessThanEqualCompareConditionExpressionExecutorFloatFloat.class,
                byteCode.new PrivateLessThanEqualCompareConditionExpressionExecutorFloatFloatBytecodeEmitter());
        byteCodegenerators.put(LessThanEqualCompareConditionExpressionExecutorFloatInt.class,
                byteCode.new PrivateLessThanEqualCompareConditionExpressionExecutorFloatIntegerBytecodeEmitter());
        byteCodegenerators.put(LessThanEqualCompareConditionExpressionExecutorFloatLong.class,
                byteCode.new PrivateLessThanEqualCompareConditionExpressionExecutorFloatLongBytecodeEmitter());
        byteCodegenerators.put(LessThanEqualCompareConditionExpressionExecutorIntDouble.class,
                byteCode.new PrivateLessThanEqualCompareConditionExpressionExecutorIntegerDoubleBytecodeEmitter());
        byteCodegenerators.put(LessThanEqualCompareConditionExpressionExecutorIntFloat.class,
                byteCode.new PrivateLessThanEqualCompareConditionExpressionExecutorIntegerFloatBytecodeEmitter());
        byteCodegenerators.put(LessThanEqualCompareConditionExpressionExecutorIntInt.class,
                byteCode.new PrivateLessThanEqualCompareConditionExpressionExecutorIntegerIntegerBytecodeEmitter());
        byteCodegenerators.put(LessThanEqualCompareConditionExpressionExecutorIntLong.class,
                byteCode.new PrivateLessThanEqualCompareConditionExpressionExecutorIntegerLongBytecodeEmitter());
        byteCodegenerators.put(LessThanEqualCompareConditionExpressionExecutorLongDouble.class,
                byteCode.new PrivateLessThanEqualCompareConditionExpressionExecutorLongDoubleBytecodeEmitter());
        byteCodegenerators.put(LessThanEqualCompareConditionExpressionExecutorLongFloat.class,
                byteCode.new PrivateLessThanEqualCompareConditionExpressionExecutorLongFloatBytecodeEmitter());
        byteCodegenerators.put(LessThanEqualCompareConditionExpressionExecutorLongInt.class,
                byteCode.new PrivateLessThanEqualCompareConditionExpressionExecutorLongIntegerBytecodeEmitter());
        byteCodegenerators.put(LessThanEqualCompareConditionExpressionExecutorLongLong.class,
                byteCode.new PrivateLessThanEqualCompareConditionExpressionExecutorLongLongBytecodeEmitter());
        // Equal Operator.
        byteCodegenerators.put(EqualCompareConditionExpressionExecutorBoolBool.class,
                byteCode.new PrivateEqualCompareConditionExpressionExecutorBoolBoolBytecodeEmitter());
        byteCodegenerators.put(EqualCompareConditionExpressionExecutorDoubleDouble.class,
                byteCode.new PrivateEqualCompareConditionExpressionExecutorDoubleDoubleBytecodeEmitter());
        byteCodegenerators.put(EqualCompareConditionExpressionExecutorDoubleFloat.class,
                byteCode.new PrivateEqualCompareConditionExpressionExecutorDoubleFloatBytecodeEmitter());
        byteCodegenerators.put(EqualCompareConditionExpressionExecutorDoubleInt.class,
                byteCode.new PrivateEqualCompareConditionExpressionExecutorDoubleIntegerBytecodeEmitter());
        byteCodegenerators.put(EqualCompareConditionExpressionExecutorDoubleLong.class,
                byteCode.new PrivateEqualCompareConditionExpressionExecutorDoubleLongBytecodeEmitter());
        byteCodegenerators.put(EqualCompareConditionExpressionExecutorFloatDouble.class,
                byteCode.new PrivateEqualCompareConditionExpressionExecutorFloatDoubleBytecodeEmitter());
        byteCodegenerators.put(EqualCompareConditionExpressionExecutorFloatFloat.class,
                byteCode.new PrivateEqualCompareConditionExpressionExecutorFloatFloatBytecodeEmitter());
        byteCodegenerators.put(EqualCompareConditionExpressionExecutorFloatInt.class,
                byteCode.new PrivateEqualCompareConditionExpressionExecutorFloatIntBytecodeEmitter());
        byteCodegenerators.put(EqualCompareConditionExpressionExecutorFloatLong.class,
                byteCode.new PrivateEqualCompareConditionExpressionExecutorFloatLongBytecodeEmitter());
        byteCodegenerators.put(EqualCompareConditionExpressionExecutorIntDouble.class,
                byteCode.new PrivateEqualCompareConditionExpressionExecutorIntegerDoubleBytecodeEmitter());
        byteCodegenerators.put(EqualCompareConditionExpressionExecutorIntFloat.class,
                byteCode.new PrivateEqualCompareConditionExpressionExecutorIntegerFloatBytecodeEmitter());
        byteCodegenerators.put(EqualCompareConditionExpressionExecutorIntInt.class,
                byteCode.new PrivateEqualCompareConditionExpressionExecutorIntegerIntegerBytecodeEmitter());
        byteCodegenerators.put(EqualCompareConditionExpressionExecutorIntLong.class,
                byteCode.new PrivateEqualCompareConditionExpressionExecutorIntegerLongBytecodeEmitter());
        byteCodegenerators.put(EqualCompareConditionExpressionExecutorLongDouble.class,
                byteCode.new PrivateEqualCompareConditionExpressionExecutorLongDoubleBytecodeEmitter());
        byteCodegenerators.put(EqualCompareConditionExpressionExecutorLongFloat.class,
                byteCode.new PrivateEqualCompareConditionExpressionExecutorLongFloatBytecodeEmitter());
        byteCodegenerators.put(EqualCompareConditionExpressionExecutorLongInt.class,
                byteCode.new PrivateEqualCompareConditionExpressionExecutorLongIntegerBytecodeEmitter());
        byteCodegenerators.put(EqualCompareConditionExpressionExecutorLongLong.class,
                byteCode.new PrivateEqualCompareConditionExpressionExecutorLongLongBytecodeEmitter());
        byteCodegenerators.put(EqualCompareConditionExpressionExecutorStringString.class,
                byteCode.new PrivateEqualCompareConditionExpressionExecutorStringStringBytecodeEmitter());
        // Not Equal Operator.
        byteCodegenerators.put(NotEqualCompareConditionExpressionExecutorBoolBool.class,
                byteCode.new PrivateNotEqualCompareConditionExpressionExecutorBoolBoolBytecodeEmitter());
        byteCodegenerators.put(NotEqualCompareConditionExpressionExecutorDoubleDouble.class,
                byteCode.new PrivateNotEqualCompareConditionExpressionExecutorDoubleDoubleBytecodeEmitter());
        byteCodegenerators.put(NotEqualCompareConditionExpressionExecutorDoubleFloat.class,
                byteCode.new PrivateNotEqualCompareConditionExpressionExecutorDoubleFloatBytecodeEmitter());
        byteCodegenerators.put(NotEqualCompareConditionExpressionExecutorDoubleInt.class,
                byteCode.new PrivateNotEqualCompareConditionExpressionExecutorDoubleIntegerBytecodeEmitter());
        byteCodegenerators.put(NotEqualCompareConditionExpressionExecutorDoubleLong.class,
                byteCode.new PrivateNotEqualCompareConditionExpressionExecutorDoubleLongBytecodeEmitter());
        byteCodegenerators.put(NotEqualCompareConditionExpressionExecutorFloatDouble.class,
                byteCode.new PrivateNotEqualCompareConditionExpressionExecutorFloatDoubleBytecodeEmitter());
        byteCodegenerators.put(NotEqualCompareConditionExpressionExecutorFloatFloat.class,
                byteCode.new PrivateNotEqualCompareConditionExpressionExecutorFloatFloatBytecodeEmitter());
        byteCodegenerators.put(NotEqualCompareConditionExpressionExecutorFloatInt.class,
                byteCode.new PrivateNotEqualCompareConditionExpressionExecutorFloatIntBytecodeEmitter());
        byteCodegenerators.put(NotEqualCompareConditionExpressionExecutorFloatLong.class,
                byteCode.new PrivateNotEqualCompareConditionExpressionExecutorFloatLongBytecodeEmitter());
        byteCodegenerators.put(NotEqualCompareConditionExpressionExecutorIntDouble.class,
                byteCode.new PrivateNotEqualCompareConditionExpressionExecutorIntegerDoubleBytecodeEmitter());
        byteCodegenerators.put(NotEqualCompareConditionExpressionExecutorIntFloat.class,
                byteCode.new PrivateNotEqualCompareConditionExpressionExecutorIntegerFloatBytecodeEmitter());
        byteCodegenerators.put(NotEqualCompareConditionExpressionExecutorIntInt.class,
                byteCode.new PrivateNotEqualCompareConditionExpressionExecutorIntegerIntegerBytecodeEmitter());
        byteCodegenerators.put(NotEqualCompareConditionExpressionExecutorIntLong.class,
                byteCode.new PrivateNotEqualCompareConditionExpressionExecutorIntegerLongBytecodeEmitter());
        byteCodegenerators.put(NotEqualCompareConditionExpressionExecutorLongDouble.class,
                byteCode.new PrivateNotEqualCompareConditionExpressionExecutorLongDoubleBytecodeEmitter());
        byteCodegenerators.put(NotEqualCompareConditionExpressionExecutorLongFloat.class,
                byteCode.new PrivateNotEqualCompareConditionExpressionExecutorLongFloatBytecodeEmitter());
        byteCodegenerators.put(NotEqualCompareConditionExpressionExecutorLongInt.class,
                byteCode.new PrivateNotEqualCompareConditionExpressionExecutorLongIntegerBytecodeEmitter());
        byteCodegenerators.put(NotEqualCompareConditionExpressionExecutorLongLong.class,
                byteCode.new PrivateNotEqualCompareConditionExpressionExecutorLongLongBytecodeEmitter());
        byteCodegenerators.put(NotEqualCompareConditionExpressionExecutorStringString.class,
                byteCode.new PrivateNotEqualCompareConditionExpressionExecutorStringStringBytecodeEmitter());
        //Mathematical Operators.
        byteCodegenerators.put(SubtractExpressionExecutorDouble.class,
                byteCode.new PrivateSubtractExpressionExecutorDoubleBytecodeEmitter());
        byteCodegenerators.put(SubtractExpressionExecutorFloat.class,
                byteCode.new PrivateSubtractExpressionExecutorFloatBytecodeEmitter());
        byteCodegenerators.put(SubtractExpressionExecutorInt.class,
                byteCode.new PrivateSubtractExpressionExecutorIntBytecodeEmitter());
        byteCodegenerators.put(SubtractExpressionExecutorLong.class,
                byteCode.new PrivateSubtractExpressionExecutorLongBytecodeEmitter());

        byteCodegenerators.put(AddExpressionExecutorDouble.class,
                byteCode.new PrivateAddExpressionExecutorDoubleBytecodeEmitter());
        byteCodegenerators.put(AddExpressionExecutorFloat.class,
                byteCode.new PrivateAddExpressionExecutorFloatBytecodeEmitter());
        byteCodegenerators.put(AddExpressionExecutorInt.class,
                byteCode.new PrivateAddExpressionExecutorIntBytecodeEmitter());
        byteCodegenerators.put(AddExpressionExecutorLong.class,
                byteCode.new PrivateAddExpressionExecutorLongBytecodeEmitter());

        byteCodegenerators.put(MultiplyExpressionExecutorDouble.class,
                byteCode.new PrivateMultiplyExpressionExecutorDoubleBytecodeEmitter());
        byteCodegenerators.put(MultiplyExpressionExecutorFloat.class,
                byteCode.new PrivateMultiplyExpressionExecutorFloatBytecodeEmitter());
        byteCodegenerators.put(MultiplyExpressionExecutorInt.class,
                byteCode.new PrivateMultiplyExpressionExecutorIntBytecodeEmitter());
        byteCodegenerators.put(MultiplyExpressionExecutorLong.class,
                byteCode.new PrivateMultiplyExpressionExecutorLongBytecodeEmitter());

        byteCodegenerators.put(DivideExpressionExecutorDouble.class,
                byteCode.new PrivateDivideExpressionExecutorDoubleBytecodeEmitter());
        byteCodegenerators.put(DivideExpressionExecutorFloat.class,
                byteCode.new PrivateDivideExpressionExecutorFloatBytecodeEmitter());
        byteCodegenerators.put(DivideExpressionExecutorInt.class,
                byteCode.new PrivateDivideExpressionExecutorIntegerBytecodeEmitter());
        byteCodegenerators.put(DivideExpressionExecutorLong.class,
                byteCode.new PrivateDivideExpressionExecutorLongBytecodeEmitter());

        byteCodegenerators.put(ModExpressionExecutorDouble.class,
                byteCode.new PrivateModExpressionExecutorDoubleBytecodeEmitter());
        byteCodegenerators.put(ModExpressionExecutorFloat.class,
                byteCode.new PrivateModExpressionExecutorFloatBytecodeEmitter());
        byteCodegenerators.put(ModExpressionExecutorInt.class,
                byteCode.new PrivateModExpressionExecutorIntegerBytecodeEmitter());
        byteCodegenerators.put(ModExpressionExecutorLong.class,
                byteCode.new PrivateModExpressionExecutorLongBytecodeEmitter());
        //Unhandled ExpressionExecutors.
        byteCodegenerators.put(ExpressionExecutor.class,
                byteCode.new PrivateExtensionBytecodeEmitter());
    }

    protected ArrayList<ExpressionExecutor> unHandledExpressionExecutors = new ArrayList<ExpressionExecutor>();
    protected int unHandledExpressionExecutorIndex = 0;
    protected byte[] byteArray;
    protected ExpressionExecutor expressionExecutor;
    private ClassWriter classWriter;
    private ByteCodeHelper byteCodeHelper;

    /**
     * This method returns Expression executor with byte code.
     *
     * @param conditionExecutor
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public ExpressionExecutor build(ExpressionExecutor conditionExecutor) {
        this.byteCodeHelper = new ByteCodeHelper();
        this.classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
        MethodVisitor methodVisitor = byteCodeHelper.start(classWriter);
        this.execute(conditionExecutor, 2, 0, null, methodVisitor, this);
        this.byteCodeHelper.end(classWriter, methodVisitor, this, conditionExecutor);
        return this.expressionExecutor;
    }

    /**
     * This method maps ExpressionExecutor to it's ByteCodeEmitter.
     * @param conditionExecutor
     * @param index
     * @param parent
     * @param specialCase
     * @param methodVisitor
     * @param byteCodeGenerator
     */
    public void execute(ExpressionExecutor conditionExecutor, int index, int parent,
                        Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
        if (ByteCodeGenerator.byteCodegenerators.containsKey(conditionExecutor.getClass())) {
            ByteCodeGenerator.byteCodegenerators.get(conditionExecutor.getClass()).generate(conditionExecutor, index,
                    parent, specialCase, methodVisitor, byteCodeGenerator);
        } else {
            ByteCodeGenerator.byteCodegenerators.get(ExpressionExecutor.class).generate(conditionExecutor, index,
                    parent, specialCase, methodVisitor, byteCodeGenerator);
        }
    }
}
