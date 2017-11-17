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
package org.wso2.siddhi.core.query.processor.filter;

import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.executor.condition.AndConditionExpressionExecutor;
import org.wso2.siddhi.core.executor.condition.BoolConditionExpressionExecutor;
import org.wso2.siddhi.core.executor.condition.IsNullConditionExpressionExecutor;
import org.wso2.siddhi.core.executor.condition.IsNullStreamConditionExpressionExecutor;
import org.wso2.siddhi.core.executor.condition.NotConditionExpressionExecutor;
import org.wso2.siddhi.core.executor.condition.OrConditionExpressionExecutor;
import org.wso2.siddhi.core.executor.condition.compare.CompareConditionExpressionExecutor;
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
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
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

import static org.objectweb.asm.Opcodes.ACONST_NULL;
import static org.objectweb.asm.Opcodes.ALOAD;
import static org.objectweb.asm.Opcodes.ASTORE;
import static org.objectweb.asm.Opcodes.BIPUSH;
import static org.objectweb.asm.Opcodes.CHECKCAST;
import static org.objectweb.asm.Opcodes.DADD;
import static org.objectweb.asm.Opcodes.DCMPG;
import static org.objectweb.asm.Opcodes.DCMPL;
import static org.objectweb.asm.Opcodes.DCONST_0;
import static org.objectweb.asm.Opcodes.DDIV;
import static org.objectweb.asm.Opcodes.DLOAD;
import static org.objectweb.asm.Opcodes.DMUL;
import static org.objectweb.asm.Opcodes.DREM;
import static org.objectweb.asm.Opcodes.DSTORE;
import static org.objectweb.asm.Opcodes.DSUB;
import static org.objectweb.asm.Opcodes.DUP;
import static org.objectweb.asm.Opcodes.F2D;
import static org.objectweb.asm.Opcodes.FADD;
import static org.objectweb.asm.Opcodes.FCMPG;
import static org.objectweb.asm.Opcodes.FCMPL;
import static org.objectweb.asm.Opcodes.FCONST_0;
import static org.objectweb.asm.Opcodes.FDIV;
import static org.objectweb.asm.Opcodes.FLOAD;
import static org.objectweb.asm.Opcodes.FMUL;
import static org.objectweb.asm.Opcodes.FREM;
import static org.objectweb.asm.Opcodes.FSTORE;
import static org.objectweb.asm.Opcodes.FSUB;
import static org.objectweb.asm.Opcodes.GETFIELD;
import static org.objectweb.asm.Opcodes.GOTO;
import static org.objectweb.asm.Opcodes.I2D;
import static org.objectweb.asm.Opcodes.I2F;
import static org.objectweb.asm.Opcodes.I2L;
import static org.objectweb.asm.Opcodes.IADD;
import static org.objectweb.asm.Opcodes.IASTORE;
import static org.objectweb.asm.Opcodes.ICONST_0;
import static org.objectweb.asm.Opcodes.ICONST_1;
import static org.objectweb.asm.Opcodes.ICONST_2;
import static org.objectweb.asm.Opcodes.ICONST_4;
import static org.objectweb.asm.Opcodes.IDIV;
import static org.objectweb.asm.Opcodes.IFEQ;
import static org.objectweb.asm.Opcodes.IFGE;
import static org.objectweb.asm.Opcodes.IFGT;
import static org.objectweb.asm.Opcodes.IFLE;
import static org.objectweb.asm.Opcodes.IFLT;
import static org.objectweb.asm.Opcodes.IFNE;
import static org.objectweb.asm.Opcodes.IFNONNULL;
import static org.objectweb.asm.Opcodes.IFNULL;
import static org.objectweb.asm.Opcodes.IF_ACMPEQ;
import static org.objectweb.asm.Opcodes.IF_ACMPNE;
import static org.objectweb.asm.Opcodes.IF_ICMPEQ;
import static org.objectweb.asm.Opcodes.IF_ICMPGE;
import static org.objectweb.asm.Opcodes.IF_ICMPGT;
import static org.objectweb.asm.Opcodes.IF_ICMPLE;
import static org.objectweb.asm.Opcodes.IF_ICMPLT;
import static org.objectweb.asm.Opcodes.IF_ICMPNE;
import static org.objectweb.asm.Opcodes.ILOAD;
import static org.objectweb.asm.Opcodes.IMUL;
import static org.objectweb.asm.Opcodes.INVOKEINTERFACE;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;
import static org.objectweb.asm.Opcodes.INVOKEVIRTUAL;
import static org.objectweb.asm.Opcodes.IREM;
import static org.objectweb.asm.Opcodes.ISTORE;
import static org.objectweb.asm.Opcodes.ISUB;
import static org.objectweb.asm.Opcodes.L2D;
import static org.objectweb.asm.Opcodes.L2F;
import static org.objectweb.asm.Opcodes.LADD;
import static org.objectweb.asm.Opcodes.LCMP;
import static org.objectweb.asm.Opcodes.LCONST_0;
import static org.objectweb.asm.Opcodes.LDIV;
import static org.objectweb.asm.Opcodes.LLOAD;
import static org.objectweb.asm.Opcodes.LMUL;
import static org.objectweb.asm.Opcodes.LREM;
import static org.objectweb.asm.Opcodes.LSTORE;
import static org.objectweb.asm.Opcodes.LSUB;
import static org.objectweb.asm.Opcodes.NEWARRAY;
import static org.objectweb.asm.Opcodes.T_INT;


/**
 * This is an outer class that consists of classes that has  implemented interface "ByteCodeEmitter".
 */
public class ByteCodeRegistry {
    /**
     * This class generates byte code for "AND" operator.
     */
    class PrivateANDExpressionExecutorBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((AndConditionExpressionExecutor) conditionExecutor).getLeftConditionExecutor();
            ExpressionExecutor right = ((AndConditionExpressionExecutor) conditionExecutor).getRightConditionExecutor();
            Label l0 = new Label();
            Label l1 = new Label();
            Label l2 = new Label();
            Label l3 = new Label();
            Label l4 = new Label();
            byteCodeGenerator.execute(left, index, 1, l1, methodVisitor, byteCodeGenerator);
            if (left instanceof VariableExpressionExecutor || left instanceof FunctionExecutor) { /* This operation
                needs to be done to check NUll values. Only checks for ExpressionExecutors for those have a possibility
                to give Null*/
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l3);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Boolean");
                methodVisitor.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Boolean", "booleanValue",
                        "()Z", false);
            }

            methodVisitor.visitJumpInsn(IFNE, l0);
            methodVisitor.visitLabel(l3);
            methodVisitor.visitInsn(ICONST_0);
            if (parent == 1) { //Checks for consecutive "AND" operators.
                methodVisitor.visitJumpInsn(GOTO, specialCase);
            } else {
                methodVisitor.visitJumpInsn(GOTO, l1);
            }

            methodVisitor.visitLabel(l0);
            byteCodeGenerator.execute(right, index, 1, l1, methodVisitor, byteCodeGenerator);
            if (right instanceof VariableExpressionExecutor || right instanceof FunctionExecutor) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l4);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Boolean");
                methodVisitor.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Boolean", "booleanValue",
                        "()Z", false);
            }

            methodVisitor.visitJumpInsn(IFNE, l2);
            methodVisitor.visitLabel(l4);
            methodVisitor.visitInsn(ICONST_0);
            if (parent == 1) {
                methodVisitor.visitJumpInsn(GOTO, specialCase);
            } else {
                methodVisitor.visitJumpInsn(GOTO, l1);
            }
            methodVisitor.visitLabel(l2);
            methodVisitor.visitInsn(ICONST_1);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "OR" operator.
     */
    class PrivateORExpressionExecutorBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((OrConditionExpressionExecutor) conditionExecutor).getLeftConditionExecutor();
            ExpressionExecutor right = ((OrConditionExpressionExecutor) conditionExecutor).getRightConditionExecutor();
            Label l0 = new Label();
            Label l1 = new Label();
            Label l2 = new Label();
            byteCodeGenerator.execute(left, index, 2, l1, methodVisitor, byteCodeGenerator);
            if (left instanceof VariableExpressionExecutor || left instanceof FunctionExecutor) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Boolean");
                methodVisitor.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Boolean", "booleanValue",
                        "()Z", false);
            }

            methodVisitor.visitJumpInsn(IFEQ, l0);
            methodVisitor.visitInsn(ICONST_1);
            if (parent == 2) { //Checks for consecutive "OR" operators.
                methodVisitor.visitJumpInsn(GOTO, specialCase);
            } else {
                methodVisitor.visitJumpInsn(GOTO, l1);
            }

            methodVisitor.visitLabel(l0);
            byteCodeGenerator.execute(right, index, 2, l1, methodVisitor, byteCodeGenerator);
            if (right instanceof VariableExpressionExecutor || right instanceof FunctionExecutor) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l2);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Boolean");
                methodVisitor.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Boolean", "booleanValue",
                        "()Z", false);
            }

            methodVisitor.visitJumpInsn(IFEQ, l2);
            methodVisitor.visitInsn(ICONST_1);
            if (parent == 2) {
                methodVisitor.visitJumpInsn(GOTO, specialCase);
            } else {
                methodVisitor.visitJumpInsn(GOTO, l1);
            }

            methodVisitor.visitLabel(l2);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "NOT" operator.
     */
    class PrivateNOTExpressionExecutorBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor condition = ((NotConditionExpressionExecutor) conditionExecutor).getConditionExecutor();
            Label l0 = new Label();
            Label l1 = new Label();
            Label l2 = new Label();
            byteCodeGenerator.execute(condition, index, 3, l1, methodVisitor, byteCodeGenerator);
            if (condition instanceof VariableExpressionExecutor || condition instanceof FunctionExecutor) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l2);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Boolean");
                methodVisitor.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Boolean", "booleanValue",
                        "()Z", false);
            }

            if (parent == 3) { //Checks for consecutive "NOT" operators.
                methodVisitor.visitJumpInsn(GOTO, specialCase);
            } else {
                methodVisitor.visitJumpInsn(IFNE, l0);
                methodVisitor.visitLabel(l2);
                methodVisitor.visitInsn(ICONST_1);
                methodVisitor.visitJumpInsn(GOTO, l1);
                methodVisitor.visitLabel(l0);
                methodVisitor.visitInsn(ICONST_0);
                methodVisitor.visitLabel(l1);
            }
        }
    }

    /**
     * This class generates byte code for "IsNull" operator.
     */
    class PrivateIsNullExpressionExecutorBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor condition = ((IsNullConditionExpressionExecutor) conditionExecutor).
                    getConditionExecutor();
            byteCodeGenerator.execute(condition, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            Label l1 = new Label();
            methodVisitor.visitVarInsn(ALOAD, index);
            methodVisitor.visitJumpInsn(IFNONNULL, l0);
            methodVisitor.visitInsn(ICONST_1);
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "IsNullStream" operator.
     */
    class PrivateIsNullStreamExpressionExecutorBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            int[] eventPosition = ((IsNullStreamConditionExpressionExecutor) conditionExecutor).getEventPosition();
            Label l0 = new Label();
            Label l1 = new Label();
            if (eventPosition.length == 0) {
                methodVisitor.visitVarInsn(ALOAD, 1);
                methodVisitor.visitJumpInsn(IFNONNULL, l0);
                methodVisitor.visitInsn(ICONST_1);
                methodVisitor.visitJumpInsn(GOTO, l1);
            } else {
                methodVisitor.visitInsn(ICONST_2);
                methodVisitor.visitIntInsn(NEWARRAY, T_INT);
                for (int i = 0; i < 2; i++) {
                    methodVisitor.visitInsn(DUP);
                    methodVisitor.visitIntInsn(BIPUSH, i);
                    methodVisitor.visitIntInsn(BIPUSH, eventPosition[i]);
                    methodVisitor.visitInsn(IASTORE);
                }

                methodVisitor.visitVarInsn(ASTORE, index);
                methodVisitor.visitVarInsn(ALOAD, 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "org/wso2/siddhi/core/event/state/StateEvent");
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitMethodInsn(INVOKEVIRTUAL, "org/wso2/siddhi/core/event/state/StateEvent",
                        "getStreamEvent", "([I)Lorg/wso2/siddhi/core/event/stream/StreamEvent;", false);

                methodVisitor.visitJumpInsn(IFNONNULL, l0);
                methodVisitor.visitInsn(ICONST_1);
                methodVisitor.visitJumpInsn(GOTO, l1);
            }

            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "Bool" operator.
     */
    class PrivateBoolExpressionExecutorBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor condition = ((BoolConditionExpressionExecutor) conditionExecutor).getConditionExecutor();
            byteCodeGenerator.execute(condition, index, 0, null, methodVisitor,
                    byteCodeGenerator);
            Label l0 = new Label();
            Label l1 = new Label();
            if (!(condition instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Boolean");
                methodVisitor.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Boolean", "booleanValue",
                        "()Z", false);
            }
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for InConditionExpressionExecutor.
     */
    class PrivateInConditionExpressionExecutorBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            byteCodeGenerator.getUnHandledExpressionExecutors().add(conditionExecutor);
            methodVisitor.visitVarInsn(ALOAD, 0);
            methodVisitor.visitFieldInsn(GETFIELD, "JITCompiledExpressionExecutor", "unHandledExpressionExecutors",
                    "Ljava/util/List;");
            methodVisitor.visitLdcInsn((byteCodeGenerator.getUnHandledExpressionExecutors().size()) - 1);
            methodVisitor.visitMethodInsn(INVOKEINTERFACE, "java/util/List", "get", "(I)Ljava/lang/Object;", true);
            methodVisitor.visitTypeInsn(CHECKCAST,
                    "org/wso2/siddhi/core/executor/condition/InConditionExpressionExecutor");
            methodVisitor.visitVarInsn(ALOAD, 1);
            methodVisitor.visitMethodInsn(INVOKEVIRTUAL,
                    "org/wso2/siddhi/core/executor/condition/InConditionExpressionExecutor",
                    "execute", "(Lorg/wso2/siddhi/core/event/ComplexEvent;)Ljava/lang/Boolean;", false);
            methodVisitor.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Boolean", "booleanValue", "()Z",
                    false);
        }
    }

    /**
     * This class generates byte code for 'Siddhi' extensions.
     */
    class PrivateExtensionBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            byteCodeGenerator.getUnHandledExpressionExecutors().add(conditionExecutor);
            methodVisitor.visitVarInsn(ALOAD, 0);
            methodVisitor.visitFieldInsn(GETFIELD, "JITCompiledExpressionExecutor", "unHandledExpressionExecutors",
                    "Ljava/util/List;");
            methodVisitor.visitLdcInsn((byteCodeGenerator.getUnHandledExpressionExecutors().size()) - 1);
            methodVisitor.visitMethodInsn(INVOKEINTERFACE, "java/util/List", "get", "(I)Ljava/lang/Object;", true);
            methodVisitor.visitVarInsn(ALOAD, 1);
            methodVisitor.visitMethodInsn(INVOKEINTERFACE, "org/wso2/siddhi/core/executor/ExpressionExecutor",
                    "execute", "(Lorg/wso2/siddhi/core/event/ComplexEvent;)Ljava/lang/Object;", true);

            methodVisitor.visitVarInsn(ASTORE, index);
        }
    }

    /**
     * This class generates byte code to take data from an event.
     */
    class PrivateVariableExpressionExecutorBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            int[] variablePosition = ((VariableExpressionExecutor) conditionExecutor).getPosition();
            methodVisitor.visitInsn(ICONST_4);
            methodVisitor.visitIntInsn(NEWARRAY, T_INT);
            for (int i = 0; i < 4; i++) {
                methodVisitor.visitInsn(DUP);
                methodVisitor.visitIntInsn(BIPUSH, i);
                methodVisitor.visitIntInsn(BIPUSH, variablePosition[i]);
                methodVisitor.visitInsn(IASTORE);
            }

            methodVisitor.visitVarInsn(ASTORE, index);
            methodVisitor.visitVarInsn(ALOAD, 1);
            methodVisitor.visitVarInsn(ALOAD, index);
            methodVisitor.visitMethodInsn(INVOKEINTERFACE, "org/wso2/siddhi/core/event/ComplexEvent",
                    "getAttribute", "([I)Ljava/lang/Object;", true);
            methodVisitor.visitVarInsn(ASTORE, index);
        }
    }

    /**
     * This class generates byte code to take value from an expression
     */
    class PrivateConstantExpressionExecutorBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            Object constantVariable = ((ConstantExpressionExecutor) conditionExecutor).getValue();
            methodVisitor.visitLdcInsn(constantVariable);
        }
    }

    /**
     * This class generates byte code for ">" operator with float on left and double on right.
     */
    class PrivateGreaterThanCompareConditionExpressionExecutorFloatDoubleBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanCompareConditionExpressionExecutorFloatDouble) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanCompareConditionExpressionExecutorFloatDouble) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitInsn(F2D);
            methodVisitor.visitVarInsn(DLOAD, index + 1);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFLE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">=" operator with float on left and double on right.
     */
    class PrivateGreaterThanEqualCompareConditionExpressionExecutorFloatDoubleBytecodeEmitter implements
            ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanEqualCompareConditionExpressionExecutorFloatDouble)
                    conditionExecutor).getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanEqualCompareConditionExpressionExecutorFloatDouble)
                    conditionExecutor).getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitInsn(F2D);
            methodVisitor.visitVarInsn(DLOAD, index + 1);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFLT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">" operator with float on left and float on right.
     */
    class PrivateGreaterThanCompareConditionExpressionExecutorFloatFloatBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanCompareConditionExpressionExecutorFloatFloat) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanCompareConditionExpressionExecutorFloatFloat) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitVarInsn(FLOAD, index + 1);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFLE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">=" operator with float on left and float on right.
     */
    class PrivateGreaterThanEqualCompareConditionExpressionExecutorFloatFloatBytecodeEmitter implements
            ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanEqualCompareConditionExpressionExecutorFloatFloat)
                    conditionExecutor).getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanEqualCompareConditionExpressionExecutorFloatFloat)
                    conditionExecutor).getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitVarInsn(FLOAD, index + 1);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFLT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">" operator with float on left and int on right.
     */
    class PrivateGreaterThanCompareConditionExpressionExecutorFloatIntegerBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanCompareConditionExpressionExecutorFloatInt) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanCompareConditionExpressionExecutorFloatInt) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 1);
            methodVisitor.visitInsn(I2F);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFLE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">=" operator with float on left and int on right.
     */
    class PrivateGreaterThanEqualCompareConditionExpressionExecutorFloatIntegerBytecodeEmitter implements
            ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanEqualCompareConditionExpressionExecutorFloatInt) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanEqualCompareConditionExpressionExecutorFloatInt) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 1);
            methodVisitor.visitInsn(I2F);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFLT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">" operator with float on left and long on right.
     */
    class PrivateGreaterThanCompareConditionExpressionExecutorFloatLongBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanCompareConditionExpressionExecutorFloatLong) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanCompareConditionExpressionExecutorFloatLong) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitVarInsn(LLOAD, index + 1);
            methodVisitor.visitInsn(L2F);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFLE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">=" operator with float on left and long on right.
     */
    class PrivateGreaterThanEqualCompareConditionExpressionExecutorFloatLongBytecodeEmitter implements
            ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanEqualCompareConditionExpressionExecutorFloatLong) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanEqualCompareConditionExpressionExecutorFloatLong) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitVarInsn(LLOAD, index + 1);
            methodVisitor.visitInsn(L2F);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFLT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">" operator with int on left and float on right.
     */
    class PrivateGreaterThanCompareConditionExpressionExecutorIntegerFloatBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanCompareConditionExpressionExecutorIntFloat) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanCompareConditionExpressionExecutorIntFloat) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitInsn(I2F);
            methodVisitor.visitVarInsn(FLOAD, index + 1);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFLE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">=" operator with int on left and float on right.
     */
    class PrivateGreaterThanEqualCompareConditionExpressionExecutorIntegerFloatBytecodeEmitter implements
            ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanEqualCompareConditionExpressionExecutorIntFloat) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanEqualCompareConditionExpressionExecutorIntFloat) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitInsn(I2F);
            methodVisitor.visitVarInsn(FLOAD, index + 1);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFLT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">" operator with long on left and float on right.
     */
    class PrivateGreaterThanCompareConditionExpressionExecutorLongFloatBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanCompareConditionExpressionExecutorLongFloat) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanCompareConditionExpressionExecutorLongFloat) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitInsn(L2F);
            methodVisitor.visitVarInsn(FLOAD, index + 2);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFLE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">=" operator with long on left and float on right.
     */
    class PrivateGreaterThanEqualCompareConditionExpressionExecutorLongFloatBytecodeEmitter implements
            ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanEqualCompareConditionExpressionExecutorLongFloat) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanEqualCompareConditionExpressionExecutorLongFloat) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitInsn(L2F);
            methodVisitor.visitVarInsn(FLOAD, index + 2);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFLT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">" operator with double on left and double on right.
     */
    class PrivateGreaterThanCompareConditionExpressionExecutorDoubleDoubleBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanCompareConditionExpressionExecutorDoubleDouble) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanCompareConditionExpressionExecutorDoubleDouble) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(DLOAD, index + 2);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFLE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">=" operator with double on left and double on right.
     */
    class PrivateGreaterThanEqualCompareConditionExpressionExecutorDoubleDoubleBytecodeEmitter implements
            ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanEqualCompareConditionExpressionExecutorDoubleDouble)
                    conditionExecutor).getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanEqualCompareConditionExpressionExecutorDoubleDouble)
                    conditionExecutor).getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(DLOAD, index + 2);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFLT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">" operator with int on left and double on right.
     */
    class PrivateGreaterThanCompareConditionExpressionExecutorIntegerDoubleBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanCompareConditionExpressionExecutorIntDouble) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanCompareConditionExpressionExecutorIntDouble) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitInsn(I2D);
            methodVisitor.visitVarInsn(DLOAD, index + 1);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFLE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">=" operator with int on left and double on right.
     */
    class PrivateGreaterThanEqualCompareConditionExpressionExecutorIntegerDoubleBytecodeEmitter implements
            ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanEqualCompareConditionExpressionExecutorIntDouble) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanEqualCompareConditionExpressionExecutorIntDouble) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitInsn(I2D);
            methodVisitor.visitVarInsn(DLOAD, index + 1);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFLT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">" operator with int on left and long on right.
     */
    class PrivateGreaterThanCompareConditionExpressionExecutorIntegerLongBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanCompareConditionExpressionExecutorIntLong) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanCompareConditionExpressionExecutorIntLong) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitInsn(I2L);
            methodVisitor.visitVarInsn(LLOAD, index + 1);
            methodVisitor.visitInsn(LCMP);
            methodVisitor.visitJumpInsn(IFLE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">=" operator with int on left and long on right.
     */
    class PrivateGreaterThanEqualCompareConditionExpressionExecutorIntegerLongBytecodeEmitter implements

            ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanEqualCompareConditionExpressionExecutorIntLong) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanEqualCompareConditionExpressionExecutorIntLong) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitInsn(I2L);
            methodVisitor.visitVarInsn(LLOAD, index + 1);
            methodVisitor.visitInsn(LCMP);
            methodVisitor.visitJumpInsn(IFLT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">" operator with long on left and double on right.
     */
    class PrivateGreaterThanCompareConditionExpressionExecutorLongDoubleBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanCompareConditionExpressionExecutorLongDouble) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanCompareConditionExpressionExecutorLongDouble) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitInsn(L2D);
            methodVisitor.visitVarInsn(DLOAD, index + 2);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFLE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">=" operator with long on left and double on right.
     */
    class PrivateGreaterThanEqualCompareConditionExpressionExecutorLongDoubleBytecodeEmitter implements
            ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanEqualCompareConditionExpressionExecutorLongDouble)
                    conditionExecutor).getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanEqualCompareConditionExpressionExecutorLongDouble)
                    conditionExecutor).getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitInsn(L2D);
            methodVisitor.visitVarInsn(DLOAD, index + 2);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFLT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">" operator with long on left and int on right.
     */
    class PrivateGreaterThanCompareConditionExpressionExecutorLongIntegerBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanCompareConditionExpressionExecutorLongInt) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanCompareConditionExpressionExecutorLongInt) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 2);
            methodVisitor.visitInsn(I2L);
            methodVisitor.visitInsn(LCMP);
            methodVisitor.visitJumpInsn(IFLE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">=" operator with long on left and int on right.
     */
    class PrivateGreaterThanEqualCompareConditionExpressionExecutorLongIntegerBytecodeEmitter implements
            ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanEqualCompareConditionExpressionExecutorLongInt) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanEqualCompareConditionExpressionExecutorLongInt) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 2);
            methodVisitor.visitInsn(I2L);
            methodVisitor.visitInsn(LCMP);
            methodVisitor.visitJumpInsn(IFLT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">" operator with long on left and long on right.
     */
    class PrivateGreaterThanCompareConditionExpressionExecutorLongLongBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanCompareConditionExpressionExecutorLongLong) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanCompareConditionExpressionExecutorLongLong) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitVarInsn(LLOAD, index + 2);
            methodVisitor.visitInsn(LCMP);
            methodVisitor.visitJumpInsn(IFLE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">=" operator with long on left and long on right.
     */
    class PrivateGreaterThanEqualCompareConditionExpressionExecutorLongLongBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanEqualCompareConditionExpressionExecutorLongLong) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanEqualCompareConditionExpressionExecutorLongLong) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitVarInsn(LLOAD, index + 2);
            methodVisitor.visitInsn(LCMP);
            methodVisitor.visitJumpInsn(IFLT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">" operator with int on left and int on right.
     */
    class PrivateGreaterThanCompareConditionExpressionExecutorIntegerIntegerBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanCompareConditionExpressionExecutorIntInt) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanCompareConditionExpressionExecutorIntInt) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 1);
            methodVisitor.visitJumpInsn(IF_ICMPLE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">=" operator with int on left and int on right.
     */
    class PrivateGreaterThanEqualCompareConditionExpressionExecutorIntegerIntegerBytecodeEmitter implements
            ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanEqualCompareConditionExpressionExecutorIntInt) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanEqualCompareConditionExpressionExecutorIntInt) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 1);
            methodVisitor.visitJumpInsn(IF_ICMPLT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">" operator with double on left and int on right.
     */
    class PrivateGreaterThanCompareConditionExpressionExecutorDoubleIntegerBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanCompareConditionExpressionExecutorDoubleInt) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanCompareConditionExpressionExecutorDoubleInt) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 2);
            methodVisitor.visitInsn(I2D);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFLE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">=" operator with double on left and int on right.
     */
    class PrivateGreaterThanEqualCompareConditionExpressionExecutorDoubleIntegerBytecodeEmitter implements
            ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanEqualCompareConditionExpressionExecutorDoubleInt) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanEqualCompareConditionExpressionExecutorDoubleInt) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 2);
            methodVisitor.visitInsn(I2D);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFLT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">" operator with double on left and long on right.
     */
    class PrivateGreaterThanCompareConditionExpressionExecutorDoubleLongBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanCompareConditionExpressionExecutorDoubleLong) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanCompareConditionExpressionExecutorDoubleLong) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(LLOAD, index + 2);
            methodVisitor.visitInsn(L2D);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFLE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">=" operator with double on left and long on right.
     */
    class PrivateGreaterThanEqualCompareConditionExpressionExecutorDoubleLongBytecodeEmitter implements
            ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanEqualCompareConditionExpressionExecutorDoubleLong) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanEqualCompareConditionExpressionExecutorDoubleLong)
                    conditionExecutor).getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(LLOAD, index + 2);
            methodVisitor.visitInsn(L2D);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFLT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">" operator with double on left and float on right.
     */
    class PrivateGreaterThanCompareConditionExpressionExecutorDoubleFloatBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanCompareConditionExpressionExecutorDoubleFloat) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanCompareConditionExpressionExecutorDoubleFloat) conditionExecutor)
                    .getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(FLOAD, index + 2);
            methodVisitor.visitInsn(F2D);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFLE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for ">=" operator with double on left and float on right.
     */
    class PrivateGreaterThanEqualCompareConditionExpressionExecutorDoubleFloatBytecodeEmitter implements
            ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((GreaterThanEqualCompareConditionExpressionExecutorDoubleFloat)
                    conditionExecutor).getLeftExpressionExecutor();
            ExpressionExecutor right = ((GreaterThanEqualCompareConditionExpressionExecutorDoubleFloat)
                    conditionExecutor).getRightExpressionExecutor();

            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(FLOAD, index + 2);
            methodVisitor.visitInsn(F2D);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFLT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<" operator with float on left and double on right.
     */
    class PrivateLessThanCompareConditionExpressionExecutorFloatDoubleBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanCompareConditionExpressionExecutorFloatDouble) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanCompareConditionExpressionExecutorFloatDouble) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitInsn(F2D);
            methodVisitor.visitVarInsn(DLOAD, index + 1);
            methodVisitor.visitInsn(DCMPG);
            methodVisitor.visitJumpInsn(IFGE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<=" operator with float on left and double on right.
     */
    class PrivateLessThanEqualCompareConditionExpressionExecutorFloatDoubleBytecodeEmitter implements
            ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanEqualCompareConditionExpressionExecutorFloatDouble) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanEqualCompareConditionExpressionExecutorFloatDouble) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitInsn(F2D);
            methodVisitor.visitVarInsn(DLOAD, index + 1);
            methodVisitor.visitInsn(DCMPG);
            methodVisitor.visitJumpInsn(IFGT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<" operator with float on left and long on right.
     */
    class PrivateLessThanCompareConditionExpressionExecutorFloatLongBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanCompareConditionExpressionExecutorFloatLong) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanCompareConditionExpressionExecutorFloatLong) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitVarInsn(LLOAD, index + 1);
            methodVisitor.visitInsn(L2F);
            methodVisitor.visitInsn(FCMPG);
            methodVisitor.visitJumpInsn(IFGE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<=" operator with float on left and long on right.
     */
    class PrivateLessThanEqualCompareConditionExpressionExecutorFloatLongBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanEqualCompareConditionExpressionExecutorFloatLong) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanEqualCompareConditionExpressionExecutorFloatLong) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitVarInsn(LLOAD, index + 1);
            methodVisitor.visitInsn(L2F);
            methodVisitor.visitInsn(FCMPG);
            methodVisitor.visitJumpInsn(IFGT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<" operator with long on left and long on right.
     */
    class PrivateLessThanCompareConditionExpressionExecutorLongLongBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanCompareConditionExpressionExecutorLongLong) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanCompareConditionExpressionExecutorLongLong) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitVarInsn(LLOAD, index + 2);
            methodVisitor.visitInsn(LCMP);
            methodVisitor.visitJumpInsn(IFGE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<=" operator with long on left and long on right.
     */
    class PrivateLessThanEqualCompareConditionExpressionExecutorLongLongBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanEqualCompareConditionExpressionExecutorLongLong) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanEqualCompareConditionExpressionExecutorLongLong) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitVarInsn(LLOAD, index + 2);
            methodVisitor.visitInsn(LCMP);
            methodVisitor.visitJumpInsn(IFGT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<" operator with long on left and double on right.
     */
    class PrivateLessThanCompareConditionExpressionExecutorLongDoubleBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanCompareConditionExpressionExecutorLongDouble) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanCompareConditionExpressionExecutorLongDouble) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitInsn(L2D);
            methodVisitor.visitVarInsn(DLOAD, index + 2);
            methodVisitor.visitInsn(DCMPG);
            methodVisitor.visitJumpInsn(IFGE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<=" operator with long on left and double on right.
     */
    class PrivateLessThanEqualCompareConditionExpressionExecutorLongDoubleBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanEqualCompareConditionExpressionExecutorLongDouble) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanEqualCompareConditionExpressionExecutorLongDouble) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitInsn(L2D);
            methodVisitor.visitVarInsn(DLOAD, index + 2);
            methodVisitor.visitInsn(DCMPG);
            methodVisitor.visitJumpInsn(IFGT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<" operator with int on left and double on right.
     */
    class PrivateLessThanCompareConditionExpressionExecutorIntegerDoubleBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanCompareConditionExpressionExecutorIntDouble) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanCompareConditionExpressionExecutorIntDouble) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitInsn(I2D);
            methodVisitor.visitVarInsn(DLOAD, index + 1);
            methodVisitor.visitInsn(DCMPG);
            methodVisitor.visitJumpInsn(IFGE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<=" operator with int on left and double on right.
     */
    class PrivateLessThanEqualCompareConditionExpressionExecutorIntegerDoubleBytecodeEmitter implements
            ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanEqualCompareConditionExpressionExecutorIntDouble) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanEqualCompareConditionExpressionExecutorIntDouble) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitInsn(I2D);
            methodVisitor.visitVarInsn(DLOAD, index + 1);
            methodVisitor.visitInsn(DCMPG);
            methodVisitor.visitJumpInsn(IFGT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<" operator with int on left and long on right.
     */
    class PrivateLessThanCompareConditionExpressionExecutorIntegerLongBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanCompareConditionExpressionExecutorIntLong) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanCompareConditionExpressionExecutorIntLong) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitInsn(I2L);
            methodVisitor.visitVarInsn(LLOAD, index + 1);
            methodVisitor.visitInsn(LCMP);
            methodVisitor.visitJumpInsn(IFGE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<=" operator with int on left and long on right.
     */
    class PrivateLessThanEqualCompareConditionExpressionExecutorIntegerLongBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanEqualCompareConditionExpressionExecutorIntLong) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanEqualCompareConditionExpressionExecutorIntLong) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitInsn(I2L);
            methodVisitor.visitVarInsn(LLOAD, index + 1);
            methodVisitor.visitInsn(LCMP);
            methodVisitor.visitJumpInsn(IFGT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<" operator with int on left and int on right.
     */
    class PrivateLessThanCompareConditionExpressionExecutorIntegerIntegerBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanCompareConditionExpressionExecutorIntInt) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanCompareConditionExpressionExecutorIntInt) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 1);
            methodVisitor.visitJumpInsn(IF_ICMPGE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<=" operator with int on left and int on right.
     */
    class PrivateLessThanEqualCompareConditionExpressionExecutorIntegerIntegerBytecodeEmitter implements
            ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanEqualCompareConditionExpressionExecutorIntInt) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanEqualCompareConditionExpressionExecutorIntInt) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 1);
            methodVisitor.visitJumpInsn(IF_ICMPGT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<" operator with long on left and int on right.
     */
    class PrivateLessThanCompareConditionExpressionExecutorLongIntegerBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanCompareConditionExpressionExecutorLongInt) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanCompareConditionExpressionExecutorLongInt) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 2);
            methodVisitor.visitInsn(I2L);
            methodVisitor.visitInsn(LCMP);
            methodVisitor.visitJumpInsn(IFGE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<=" operator with long on left and int on right.
     */
    class PrivateLessThanEqualCompareConditionExpressionExecutorLongIntegerBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanEqualCompareConditionExpressionExecutorLongInt) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanEqualCompareConditionExpressionExecutorLongInt) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 2);
            methodVisitor.visitInsn(I2L);
            methodVisitor.visitInsn(LCMP);
            methodVisitor.visitJumpInsn(IFGT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<" operator with int on left and float on right.
     */
    class PrivateLessThanCompareConditionExpressionExecutorIntegerFloatBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanCompareConditionExpressionExecutorIntFloat) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanCompareConditionExpressionExecutorIntFloat) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitInsn(I2F);
            methodVisitor.visitVarInsn(FLOAD, index + 1);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFGE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<=" operator with int on left and float on right.
     */
    class PrivateLessThanEqualCompareConditionExpressionExecutorIntegerFloatBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanEqualCompareConditionExpressionExecutorIntFloat) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanEqualCompareConditionExpressionExecutorIntFloat) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitInsn(I2F);
            methodVisitor.visitVarInsn(FLOAD, index + 1);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFGT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<" operator with long on left and float on right.
     */
    class PrivateLessThanCompareConditionExpressionExecutorLongFloatBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanCompareConditionExpressionExecutorLongFloat) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanCompareConditionExpressionExecutorLongFloat) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitInsn(L2F);
            methodVisitor.visitVarInsn(FLOAD, index + 2);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFGE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<=" operator with long on left and float on right.
     */
    class PrivateLessThanEqualCompareConditionExpressionExecutorLongFloatBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanEqualCompareConditionExpressionExecutorLongFloat) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanEqualCompareConditionExpressionExecutorLongFloat) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitInsn(L2F);
            methodVisitor.visitVarInsn(FLOAD, index + 2);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFGT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<" operator with float on left and int on right.
     */
    class PrivateLessThanCompareConditionExpressionExecutorFloatIntegerBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanCompareConditionExpressionExecutorFloatInt) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanCompareConditionExpressionExecutorFloatInt) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 1);
            methodVisitor.visitInsn(I2F);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFGE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<=" operator with float on left and int on right.
     */
    class PrivateLessThanEqualCompareConditionExpressionExecutorFloatIntegerBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanEqualCompareConditionExpressionExecutorFloatInt) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanEqualCompareConditionExpressionExecutorFloatInt) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 1);
            methodVisitor.visitInsn(I2F);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFGT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<" operator with float on left and float on right.
     */
    class PrivateLessThanCompareConditionExpressionExecutorFloatFloatBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanCompareConditionExpressionExecutorFloatFloat) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanCompareConditionExpressionExecutorFloatFloat) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitVarInsn(FLOAD, index + 1);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFGE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<=" operator with float on left and float on right.
     */
    class PrivateLessThanEqualCompareConditionExpressionExecutorFloatFloatBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanEqualCompareConditionExpressionExecutorFloatFloat) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanEqualCompareConditionExpressionExecutorFloatFloat) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitVarInsn(FLOAD, index + 1);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFGT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<" operator with double on left and double on right.
     */
    class PrivateLessThanCompareConditionExpressionExecutorDoubleDoubleBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanCompareConditionExpressionExecutorDoubleDouble) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanCompareConditionExpressionExecutorDoubleDouble) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(DLOAD, index + 2);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFGE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<=" operator with double on left and double on right.
     */
    class PrivateLessThanEqualCompareConditionExpressionExecutorDoubleDoubleBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanEqualCompareConditionExpressionExecutorDoubleDouble) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanEqualCompareConditionExpressionExecutorDoubleDouble) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(DLOAD, index + 2);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFGT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<" operator with double on left and long on right.
     */
    class PrivateLessThanCompareConditionExpressionExecutorDoubleLongBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanCompareConditionExpressionExecutorDoubleLong) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanCompareConditionExpressionExecutorDoubleLong) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(LLOAD, index + 2);
            methodVisitor.visitInsn(L2D);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFGE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<=" operator with double on left and long on right.
     */
    class PrivateLessThanEqualCompareConditionExpressionExecutorDoubleLongBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanEqualCompareConditionExpressionExecutorDoubleLong) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanEqualCompareConditionExpressionExecutorDoubleLong) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(LLOAD, index + 2);
            methodVisitor.visitInsn(L2D);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFGT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<" operator with double on left and int on right.
     */
    class PrivateLessThanCompareConditionExpressionExecutorDoubleIntegerBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanCompareConditionExpressionExecutorDoubleInt) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanCompareConditionExpressionExecutorDoubleInt) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 2);
            methodVisitor.visitInsn(I2D);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFGE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<=" operator with double on left and int on right.
     */
    class PrivateLessThanEqualCompareConditionExpressionExecutorDoubleIntegerBytecodeEmitter implements
            ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanEqualCompareConditionExpressionExecutorDoubleInt) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanEqualCompareConditionExpressionExecutorDoubleInt) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 2);
            methodVisitor.visitInsn(I2D);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFGT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<" operator with double on left and float on right.
     */
    class PrivateLessThanCompareConditionExpressionExecutorDoubleFloatBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanCompareConditionExpressionExecutorDoubleFloat) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanCompareConditionExpressionExecutorDoubleFloat) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(FLOAD, index + 2);
            methodVisitor.visitInsn(F2D);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFGE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "<=" operator with double on left and float on right.
     */
    class PrivateLessThanEqualCompareConditionExpressionExecutorDoubleFloatBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((LessThanEqualCompareConditionExpressionExecutorDoubleFloat) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((LessThanEqualCompareConditionExpressionExecutorDoubleFloat) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(FLOAD, index + 2);
            methodVisitor.visitInsn(F2D);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFGT, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "==" operator with boolean on left and boolean on right.
     */
    class PrivateEqualCompareConditionExpressionExecutorBoolBoolBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((EqualCompareConditionExpressionExecutorBoolBool) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((EqualCompareConditionExpressionExecutorBoolBool) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor || left instanceof CompareConditionExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Boolean");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Boolean", "booleanValue",
                        "()Z", false);
            }

            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor || right instanceof CompareConditionExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Boolean");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Boolean", "booleanValue",
                        "()Z", false);
            }

            methodVisitor.visitVarInsn(ISTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 1);
            methodVisitor.visitJumpInsn(IF_ICMPNE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "!=" operator with boolean on left and boolean on right.
     */
    class PrivateNotEqualCompareConditionExpressionExecutorBoolBoolBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((NotEqualCompareConditionExpressionExecutorBoolBool) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((NotEqualCompareConditionExpressionExecutorBoolBool) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor || left instanceof CompareConditionExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Boolean");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Boolean", "booleanValue",
                        "()Z", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor || right instanceof CompareConditionExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Boolean");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Boolean", "booleanValue",
                        "()Z", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 1);
            Label l2 = new Label();
            methodVisitor.visitJumpInsn(IF_ICMPEQ, l2);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l2);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "==" operator with double on left and double on right.
     */
    class PrivateEqualCompareConditionExpressionExecutorDoubleDoubleBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((EqualCompareConditionExpressionExecutorDoubleDouble) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((EqualCompareConditionExpressionExecutorDoubleDouble) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(DLOAD, index + 2);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFNE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "!=" operator with double on left and double on right.
     */
    class PrivateNotEqualCompareConditionExpressionExecutorDoubleDoubleBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((NotEqualCompareConditionExpressionExecutorDoubleDouble) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((NotEqualCompareConditionExpressionExecutorDoubleDouble) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(DLOAD, index + 2);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFEQ, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "==" operator with double on left and float on right.
     */
    class PrivateEqualCompareConditionExpressionExecutorDoubleFloatBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((EqualCompareConditionExpressionExecutorDoubleFloat) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((EqualCompareConditionExpressionExecutorDoubleFloat) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(FLOAD, index + 2);
            methodVisitor.visitInsn(F2D);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFNE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "!=" operator with double on left and float on right.
     */
    class PrivateNotEqualCompareConditionExpressionExecutorDoubleFloatBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((NotEqualCompareConditionExpressionExecutorDoubleFloat) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((NotEqualCompareConditionExpressionExecutorDoubleFloat) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(FLOAD, index + 2);
            methodVisitor.visitInsn(F2D);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFEQ, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "==" operator with double on left and int on right.
     */
    class PrivateEqualCompareConditionExpressionExecutorDoubleIntegerBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((EqualCompareConditionExpressionExecutorDoubleInt) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((EqualCompareConditionExpressionExecutorDoubleInt) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 2);
            methodVisitor.visitInsn(I2D);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFNE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "!=" operator with double on left and int on right.
     */
    class PrivateNotEqualCompareConditionExpressionExecutorDoubleIntegerBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((NotEqualCompareConditionExpressionExecutorDoubleInt) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((NotEqualCompareConditionExpressionExecutorDoubleInt) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 2);
            methodVisitor.visitInsn(I2D);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFEQ, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "==" operator with double on left and long on right.
     */
    class PrivateEqualCompareConditionExpressionExecutorDoubleLongBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((EqualCompareConditionExpressionExecutorDoubleLong) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((EqualCompareConditionExpressionExecutorDoubleLong) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(LLOAD, index + 2);
            methodVisitor.visitInsn(L2D);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFNE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "!=" operator with double on left and long on right.
     */
    class PrivateNotEqualCompareConditionExpressionExecutorDoubleLongBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((NotEqualCompareConditionExpressionExecutorDoubleLong) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((NotEqualCompareConditionExpressionExecutorDoubleLong) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(LLOAD, index + 2);
            methodVisitor.visitInsn(L2D);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFEQ, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "==" operator with float on left and double on right.
     */
    class PrivateEqualCompareConditionExpressionExecutorFloatDoubleBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((EqualCompareConditionExpressionExecutorFloatDouble) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((EqualCompareConditionExpressionExecutorFloatDouble) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitInsn(F2D);
            methodVisitor.visitVarInsn(DLOAD, index + 1);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFNE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "!=" operator with float on left and double on right.
     */
    class PrivateNotEqualCompareConditionExpressionExecutorFloatDoubleBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((NotEqualCompareConditionExpressionExecutorFloatDouble) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((NotEqualCompareConditionExpressionExecutorFloatDouble) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitInsn(F2D);
            methodVisitor.visitVarInsn(DLOAD, index + 1);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFEQ, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "==" operator with long on left and double on right.
     */
    class PrivateEqualCompareConditionExpressionExecutorLongDoubleBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((EqualCompareConditionExpressionExecutorLongDouble) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((EqualCompareConditionExpressionExecutorLongDouble) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitInsn(L2D);
            methodVisitor.visitVarInsn(DLOAD, index + 2);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFNE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "!=" operator with long on left and double on right.
     */
    class PrivateNotEqualCompareConditionExpressionExecutorLongDoubleBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((NotEqualCompareConditionExpressionExecutorLongDouble) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((NotEqualCompareConditionExpressionExecutorLongDouble) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitInsn(L2D);
            methodVisitor.visitVarInsn(DLOAD, index + 2);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFEQ, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "==" operator with long on left and float on right.
     */
    class PrivateEqualCompareConditionExpressionExecutorLongFloatBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((EqualCompareConditionExpressionExecutorLongFloat) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((EqualCompareConditionExpressionExecutorLongFloat) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitInsn(L2F);
            methodVisitor.visitVarInsn(FLOAD, index + 2);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFNE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "!=" operator with long on left and float on right.
     */
    class PrivateNotEqualCompareConditionExpressionExecutorLongFloatBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((NotEqualCompareConditionExpressionExecutorLongFloat) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((NotEqualCompareConditionExpressionExecutorLongFloat) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitInsn(L2F);
            methodVisitor.visitVarInsn(FLOAD, index + 2);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFEQ, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "==" operator with long on left and int on right.
     */
    class PrivateEqualCompareConditionExpressionExecutorLongIntegerBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((EqualCompareConditionExpressionExecutorLongInt) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((EqualCompareConditionExpressionExecutorLongInt) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 2);
            methodVisitor.visitInsn(I2L);
            methodVisitor.visitInsn(LCMP);
            methodVisitor.visitJumpInsn(IFNE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "!=" operator with long on left and int on right.
     */
    class PrivateNotEqualCompareConditionExpressionExecutorLongIntegerBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((NotEqualCompareConditionExpressionExecutorLongInt) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((NotEqualCompareConditionExpressionExecutorLongInt) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 2);
            methodVisitor.visitInsn(I2L);
            methodVisitor.visitInsn(LCMP);
            methodVisitor.visitJumpInsn(IFEQ, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "==" operator with long on left and long on right.
     */
    class PrivateEqualCompareConditionExpressionExecutorLongLongBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((EqualCompareConditionExpressionExecutorLongLong) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((EqualCompareConditionExpressionExecutorLongLong) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitVarInsn(LLOAD, index + 2);
            methodVisitor.visitInsn(LCMP);
            methodVisitor.visitJumpInsn(IFNE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "!=" operator with long on left and long on right.
     */
    class PrivateNotEqualCompareConditionExpressionExecutorLongLongBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((NotEqualCompareConditionExpressionExecutorLongLong) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((NotEqualCompareConditionExpressionExecutorLongLong) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitVarInsn(LLOAD, index + 2);
            methodVisitor.visitInsn(LCMP);
            methodVisitor.visitJumpInsn(IFEQ, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "==" operator with int on left and double on right.
     */
    class PrivateEqualCompareConditionExpressionExecutorIntegerDoubleBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((EqualCompareConditionExpressionExecutorIntDouble) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((EqualCompareConditionExpressionExecutorIntDouble) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitInsn(I2D);
            methodVisitor.visitVarInsn(DLOAD, index + 1);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFNE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "!=" operator with int on left and double on right.
     */
    class PrivateNotEqualCompareConditionExpressionExecutorIntegerDoubleBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((NotEqualCompareConditionExpressionExecutorIntDouble) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((NotEqualCompareConditionExpressionExecutorIntDouble) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Double");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue",
                        "()D", false);
            }
            methodVisitor.visitVarInsn(DSTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitInsn(I2D);
            methodVisitor.visitVarInsn(DLOAD, index + 1);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFEQ, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "==" operator with int on left and float on right.
     */
    class PrivateEqualCompareConditionExpressionExecutorIntegerFloatBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((EqualCompareConditionExpressionExecutorIntFloat) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((EqualCompareConditionExpressionExecutorIntFloat) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitInsn(I2F);
            methodVisitor.visitVarInsn(FLOAD, index + 1);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFNE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "!=" operator with int on left and float on right.
     */
    class PrivateNotEqualCompareConditionExpressionExecutorIntegerFloatBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((NotEqualCompareConditionExpressionExecutorIntFloat) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((NotEqualCompareConditionExpressionExecutorIntFloat) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitInsn(I2F);
            methodVisitor.visitVarInsn(FLOAD, index + 1);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFEQ, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "==" operator with int on left and int on right.
     */
    class PrivateEqualCompareConditionExpressionExecutorIntegerIntegerBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((EqualCompareConditionExpressionExecutorIntInt) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((EqualCompareConditionExpressionExecutorIntInt) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 1);
            methodVisitor.visitJumpInsn(IF_ICMPNE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "!=" operator with int on left and int on right.
     */
    class PrivateNotEqualCompareConditionExpressionExecutorIntegerIntegerBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((NotEqualCompareConditionExpressionExecutorIntInt) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((NotEqualCompareConditionExpressionExecutorIntInt) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 1);
            methodVisitor.visitJumpInsn(IF_ICMPEQ, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "==" operator with int on left and long on right.
     */
    class PrivateEqualCompareConditionExpressionExecutorIntegerLongBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((EqualCompareConditionExpressionExecutorIntLong) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((EqualCompareConditionExpressionExecutorIntLong) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitInsn(I2L);
            methodVisitor.visitVarInsn(LLOAD, index + 1);
            methodVisitor.visitInsn(LCMP);
            methodVisitor.visitJumpInsn(IFNE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "!=" operator with int on left and long on right.
     */
    class PrivateNotEqualCompareConditionExpressionExecutorIntegerLongBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((NotEqualCompareConditionExpressionExecutorIntLong) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((NotEqualCompareConditionExpressionExecutorIntLong) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitInsn(I2L);
            methodVisitor.visitVarInsn(LLOAD, index + 1);
            methodVisitor.visitInsn(LCMP);
            methodVisitor.visitJumpInsn(IFEQ, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "==" operator with float on left and float on right.
     */
    class PrivateEqualCompareConditionExpressionExecutorFloatFloatBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((EqualCompareConditionExpressionExecutorFloatFloat) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((EqualCompareConditionExpressionExecutorFloatFloat) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitVarInsn(FLOAD, index + 1);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFNE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "!=" operator with float on left and float on right.
     */
    class PrivateNotEqualCompareConditionExpressionExecutorFloatFloatBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((NotEqualCompareConditionExpressionExecutorFloatFloat) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((NotEqualCompareConditionExpressionExecutorFloatFloat) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitVarInsn(FLOAD, index + 1);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFEQ, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "==" operator with float on left and int on right.
     */
    class PrivateEqualCompareConditionExpressionExecutorFloatIntBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((EqualCompareConditionExpressionExecutorFloatInt) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((EqualCompareConditionExpressionExecutorFloatInt) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 1);
            methodVisitor.visitInsn(I2F);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFNE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "!=" operator with float on left and int on right.
     */
    class PrivateNotEqualCompareConditionExpressionExecutorFloatIntBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((NotEqualCompareConditionExpressionExecutorFloatInt) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((NotEqualCompareConditionExpressionExecutorFloatInt) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Integer");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue",
                        "()I", false);
            }
            methodVisitor.visitVarInsn(ISTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 1);
            methodVisitor.visitInsn(I2F);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFEQ, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "==" operator with float on left and long on right.
     */
    class PrivateEqualCompareConditionExpressionExecutorFloatLongBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((EqualCompareConditionExpressionExecutorFloatLong) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((EqualCompareConditionExpressionExecutorFloatLong) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitVarInsn(LLOAD, index + 1);
            methodVisitor.visitInsn(L2F);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFNE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "!=" operator with float on left and long on right.
     */
    class PrivateNotEqualCompareConditionExpressionExecutorFloatLongBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((NotEqualCompareConditionExpressionExecutorFloatLong) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((NotEqualCompareConditionExpressionExecutorFloatLong) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Float");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue",
                        "()F", false);
            }
            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Long");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue",
                        "()J", false);
            }
            methodVisitor.visitVarInsn(LSTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitVarInsn(LLOAD, index + 1);
            methodVisitor.visitInsn(L2F);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFEQ, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "==" operator with String on left and String on right.
     */
    class PrivateEqualCompareConditionExpressionExecutorStringStringBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((EqualCompareConditionExpressionExecutorStringString) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((EqualCompareConditionExpressionExecutorStringString) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
            }
            methodVisitor.visitVarInsn(ASTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
            }
            methodVisitor.visitVarInsn(ASTORE, index + 1);
            methodVisitor.visitVarInsn(ALOAD, index);
            methodVisitor.visitVarInsn(ALOAD, index + 1);
            methodVisitor.visitJumpInsn(IF_ACMPNE, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code for "!=" operator with String on left and String on right.
     */
    class PrivateNotEqualCompareConditionExpressionExecutorStringStringBytecodeEmitter implements ByteCodeEmitter {

        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((NotEqualCompareConditionExpressionExecutorStringString) conditionExecutor)
                    .getLeftExpressionExecutor();
            ExpressionExecutor right = ((NotEqualCompareConditionExpressionExecutorStringString) conditionExecutor)
                    .getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
            }
            methodVisitor.visitVarInsn(ASTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
            }
            methodVisitor.visitVarInsn(ASTORE, index + 1);
            methodVisitor.visitVarInsn(ALOAD, index);
            methodVisitor.visitVarInsn(ALOAD, index + 1);
            methodVisitor.visitJumpInsn(IF_ACMPEQ, l0);
            methodVisitor.visitInsn(ICONST_1);
            Label l1 = new Label();
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitLabel(l1);
        }
    }

    /**
     * This class generates byte code to subtract 2 doubles.
     */
    class PrivateSubtractExpressionExecutorDoubleBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((SubtractExpressionExecutorDouble) conditionExecutor).
                    getLeftExpressionExecutor();
            ExpressionExecutor right = ((SubtractExpressionExecutorDouble) conditionExecutor).
                    getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            Label l1 = new Label();

            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "doubleValue", "()D",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) left).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2D);
                } else if (constantVariable instanceof Float) {
                    methodVisitor.visitInsn(F2D);
                } else if (constantVariable instanceof Long) {
                    methodVisitor.visitInsn(L2D);
                }
            }

            methodVisitor.visitVarInsn(DSTORE, index);

            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "doubleValue", "()D",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) right).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2D);
                } else if (constantVariable instanceof Float) {
                    methodVisitor.visitInsn(F2D);
                } else if (constantVariable instanceof Long) {
                    methodVisitor.visitInsn(L2D);
                }
            }

            methodVisitor.visitVarInsn(DSTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(DLOAD, index + 2);
            methodVisitor.visitInsn(DSUB);
            methodVisitor.visitMethodInsn(INVOKESTATIC, "java/lang/Double", "valueOf",
                    "(D)Ljava/lang/Double;", false);
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ACONST_NULL);
            methodVisitor.visitLabel(l1);
            methodVisitor.visitVarInsn(ASTORE, index);
        }
    }

    /**
     * This class generates byte code to subtract 2 floats.
     */
    class PrivateSubtractExpressionExecutorFloatBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((SubtractExpressionExecutorFloat) conditionExecutor).getLeftExpressionExecutor();
            ExpressionExecutor right = ((SubtractExpressionExecutorFloat) conditionExecutor).
                    getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            Label l1 = new Label();

            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "floatValue", "()F",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) left).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2F);
                } else if (constantVariable instanceof Long) {
                    methodVisitor.visitInsn(L2F);
                }
            }

            methodVisitor.visitVarInsn(FSTORE, index);

            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "floatValue", "()F",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) right).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2F);
                } else if (constantVariable instanceof Long) {
                    methodVisitor.visitInsn(L2F);
                }
            }

            methodVisitor.visitVarInsn(FSTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitVarInsn(FLOAD, index + 1);
            methodVisitor.visitInsn(FSUB);
            methodVisitor.visitMethodInsn(INVOKESTATIC, "java/lang/Float", "valueOf",
                    "(F)Ljava/lang/Float;", false);
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ACONST_NULL);
            methodVisitor.visitLabel(l1);
            methodVisitor.visitVarInsn(ASTORE, index);
        }
    }

    /**
     * This class generates byte code to subtract 2 Integers.
     */
    class PrivateSubtractExpressionExecutorIntBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((SubtractExpressionExecutorInt) conditionExecutor).getLeftExpressionExecutor();
            ExpressionExecutor right = ((SubtractExpressionExecutorInt) conditionExecutor).getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            Label l1 = new Label();

            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "intValue", "()I",
                        false);
            }

            methodVisitor.visitVarInsn(ISTORE, index);

            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "intValue", "()I",
                        false);
            }

            methodVisitor.visitVarInsn(ISTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 1);
            methodVisitor.visitInsn(ISUB);
            methodVisitor.visitMethodInsn(INVOKESTATIC, "java/lang/Integer", "valueOf",
                    "(I)Ljava/lang/Integer;", false);
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ACONST_NULL);
            methodVisitor.visitLabel(l1);
            methodVisitor.visitVarInsn(ASTORE, index);
        }
    }

    /**
     * This class generates byte code to subtract 2 Long values.
     */
    class PrivateSubtractExpressionExecutorLongBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((SubtractExpressionExecutorLong) conditionExecutor).getLeftExpressionExecutor();
            ExpressionExecutor right = ((SubtractExpressionExecutorLong) conditionExecutor).
                    getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            Label l1 = new Label();

            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "longValue", "()J",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) left).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2L);
                }
            }

            methodVisitor.visitVarInsn(LSTORE, index);

            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "longValue", "()J",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) right).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2L);
                }
            }

            methodVisitor.visitVarInsn(LSTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitVarInsn(LLOAD, index + 2);
            methodVisitor.visitInsn(LSUB);
            methodVisitor.visitMethodInsn(INVOKESTATIC, "java/lang/Long", "valueOf",
                    "(J)Ljava/lang/Long;", false);
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ACONST_NULL);
            methodVisitor.visitLabel(l1);
            methodVisitor.visitVarInsn(ASTORE, index);

        }
    }

    /**
     * This class generates byte code to add 2 doubles.
     */
    class PrivateAddExpressionExecutorDoubleBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((AddExpressionExecutorDouble) conditionExecutor).getLeftExpressionExecutor();
            ExpressionExecutor right = ((AddExpressionExecutorDouble) conditionExecutor).getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            Label l1 = new Label();

            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number",
                        "doubleValue", "()D", false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) left).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2D);
                } else if (constantVariable instanceof Float) {
                    methodVisitor.visitInsn(F2D);
                } else if (constantVariable instanceof Long) {
                    methodVisitor.visitInsn(L2D);
                }
            }

            methodVisitor.visitVarInsn(DSTORE, index);

            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "doubleValue", "()D",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) right).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2D);
                } else if (constantVariable instanceof Float) {
                    methodVisitor.visitInsn(F2D);
                } else if (constantVariable instanceof Long) {
                    methodVisitor.visitInsn(L2D);
                }
            }

            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitInsn(DADD);
            methodVisitor.visitMethodInsn(INVOKESTATIC, "java/lang/Double", "valueOf",
                    "(D)Ljava/lang/Double;", false);
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ACONST_NULL);
            methodVisitor.visitLabel(l1);
            methodVisitor.visitVarInsn(ASTORE, index);
        }
    }

    /**
     * This class generates byte code to add 2 floats.
     */
    class PrivateAddExpressionExecutorFloatBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((AddExpressionExecutorFloat) conditionExecutor).getLeftExpressionExecutor();
            ExpressionExecutor right = ((AddExpressionExecutorFloat) conditionExecutor).getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            Label l1 = new Label();

            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "floatValue", "()F",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) left).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2F);
                } else if (constantVariable instanceof Long) {
                    methodVisitor.visitInsn(L2F);
                }
            }

            methodVisitor.visitVarInsn(FSTORE, index);

            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "floatValue",
                        "()F", false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) right).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2F);
                } else if (constantVariable instanceof Long) {
                    methodVisitor.visitInsn(L2F);
                }
            }

            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitInsn(FADD);
            methodVisitor.visitMethodInsn(INVOKESTATIC, "java/lang/Float", "valueOf",
                    "(F)Ljava/lang/Float;", false);
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ACONST_NULL);
            methodVisitor.visitLabel(l1);
            methodVisitor.visitVarInsn(ASTORE, index);

        }
    }

    /**
     * This class generates byte code to add 2 Integers.
     */
    class PrivateAddExpressionExecutorIntBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((AddExpressionExecutorInt) conditionExecutor).getLeftExpressionExecutor();
            ExpressionExecutor right = ((AddExpressionExecutorInt) conditionExecutor).getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            Label l1 = new Label();

            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "intValue", "()I",
                        false);
            }

            methodVisitor.visitVarInsn(ISTORE, index);

            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "intValue", "()I",
                        false);
            }

            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitInsn(IADD);
            methodVisitor.visitMethodInsn(INVOKESTATIC, "java/lang/Integer", "valueOf",
                    "(I)Ljava/lang/Integer;", false);
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ACONST_NULL);
            methodVisitor.visitLabel(l1);
            methodVisitor.visitVarInsn(ASTORE, index);
        }
    }

    /**
     * This class generates byte code to add 2 Long values.
     */
    class PrivateAddExpressionExecutorLongBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((AddExpressionExecutorLong) conditionExecutor).getLeftExpressionExecutor();
            ExpressionExecutor right = ((AddExpressionExecutorLong) conditionExecutor).getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            Label l1 = new Label();

            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "longValue", "()J",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) left).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2L);
                }
            }

            methodVisitor.visitVarInsn(LSTORE, index);

            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "longValue", "()J",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) right).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2L);
                }
            }

            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitInsn(LADD);
            methodVisitor.visitMethodInsn(INVOKESTATIC, "java/lang/Long", "valueOf",
                    "(J)Ljava/lang/Long;", false);
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ACONST_NULL);
            methodVisitor.visitLabel(l1);
            methodVisitor.visitVarInsn(ASTORE, index);
        }
    }

    /**
     * This class generates byte code to multiply 2 doubles.
     */
    class PrivateMultiplyExpressionExecutorDoubleBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((MultiplyExpressionExecutorDouble) conditionExecutor).
                    getLeftExpressionExecutor();
            ExpressionExecutor right = ((MultiplyExpressionExecutorDouble) conditionExecutor).
                    getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            Label l1 = new Label();

            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "doubleValue", "()D",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) left).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2D);
                } else if (constantVariable instanceof Float) {
                    methodVisitor.visitInsn(F2D);
                } else if (constantVariable instanceof Long) {
                    methodVisitor.visitInsn(L2D);
                }
            }

            methodVisitor.visitVarInsn(DSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "doubleValue", "()D",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) right).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2D);
                } else if (constantVariable instanceof Float) {
                    methodVisitor.visitInsn(F2D);
                } else if (constantVariable instanceof Long) {
                    methodVisitor.visitInsn(L2D);
                }
            }

            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitInsn(DMUL);
            methodVisitor.visitMethodInsn(INVOKESTATIC, "java/lang/Double", "valueOf",
                    "(D)Ljava/lang/Double;", false);
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ACONST_NULL);
            methodVisitor.visitLabel(l1);
            methodVisitor.visitVarInsn(ASTORE, index);
        }
    }

    /**
     * This class generates byte code to multiply 2 floats.
     */
    class PrivateMultiplyExpressionExecutorFloatBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((MultiplyExpressionExecutorFloat) conditionExecutor).getLeftExpressionExecutor();
            ExpressionExecutor right = ((MultiplyExpressionExecutorFloat) conditionExecutor).
                    getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            Label l1 = new Label();

            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "floatValue", "()F",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) left).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2F);
                } else if (constantVariable instanceof Long) {
                    methodVisitor.visitInsn(L2F);
                }
            }

            methodVisitor.visitVarInsn(FSTORE, index);
            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "floatValue", "()F",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) right).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2F);
                } else if (constantVariable instanceof Long) {
                    methodVisitor.visitInsn(L2F);
                }
            }

            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitInsn(FMUL);
            methodVisitor.visitMethodInsn(INVOKESTATIC, "java/lang/Float", "valueOf",
                    "(F)Ljava/lang/Float;", false);
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ACONST_NULL);
            methodVisitor.visitLabel(l1);
            methodVisitor.visitVarInsn(ASTORE, index);
        }
    }

    /**
     * This class generates byte code to multiply 2 Integers.
     */
    class PrivateMultiplyExpressionExecutorIntBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((MultiplyExpressionExecutorInt) conditionExecutor).getLeftExpressionExecutor();
            ExpressionExecutor right = ((MultiplyExpressionExecutorInt) conditionExecutor).getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            Label l1 = new Label();

            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "intValue", "()I",
                        false);
            }

            methodVisitor.visitVarInsn(ISTORE, index);

            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "intValue", "()I",
                        false);
            }

            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitInsn(IMUL);
            methodVisitor.visitMethodInsn(INVOKESTATIC, "java/lang/Integer", "valueOf",
                    "(I)Ljava/lang/Integer;", false);
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ACONST_NULL);
            methodVisitor.visitLabel(l1);
            methodVisitor.visitVarInsn(ASTORE, index);
        }
    }

    /**
     * This class generates byte code to multiply 2 Long values.
     */
    class PrivateMultiplyExpressionExecutorLongBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((MultiplyExpressionExecutorLong) conditionExecutor).getLeftExpressionExecutor();
            ExpressionExecutor right = ((MultiplyExpressionExecutorLong) conditionExecutor).
                    getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            Label l1 = new Label();

            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "longValue", "()J",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) left).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2L);
                }
            }

            methodVisitor.visitVarInsn(LSTORE, index);

            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "longValue", "()J",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) right).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2L);
                }
            }

            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitInsn(LMUL);
            methodVisitor.visitMethodInsn(INVOKESTATIC, "java/lang/Long", "valueOf",
                    "(J)Ljava/lang/Long;", false);
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ACONST_NULL);
            methodVisitor.visitLabel(l1);
            methodVisitor.visitVarInsn(ASTORE, index);
        }
    }

    /**
     * This class generates byte code to divide 2 doubles.
     */
    class PrivateDivideExpressionExecutorDoubleBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((DivideExpressionExecutorDouble) conditionExecutor).getLeftExpressionExecutor();
            ExpressionExecutor right = ((DivideExpressionExecutorDouble) conditionExecutor).
                    getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            Label l1 = new Label();

            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "doubleValue", "()D",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) left).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2D);
                } else if (constantVariable instanceof Float) {
                    methodVisitor.visitInsn(F2D);
                } else if (constantVariable instanceof Long) {
                    methodVisitor.visitInsn(L2D);
                }
            }

            methodVisitor.visitVarInsn(DSTORE, index);

            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "doubleValue", "()D",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) right).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2D);
                } else if (constantVariable instanceof Float) {
                    methodVisitor.visitInsn(F2D);
                } else if (constantVariable instanceof Long) {
                    methodVisitor.visitInsn(L2D);
                }
            }

            methodVisitor.visitVarInsn(DSTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index + 2);
            methodVisitor.visitInsn(DCONST_0);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFEQ, l0);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(DLOAD, index + 2);
            methodVisitor.visitInsn(DDIV);
            methodVisitor.visitMethodInsn(INVOKESTATIC, "java/lang/Double", "valueOf",
                    "(D)Ljava/lang/Double;", false);
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ACONST_NULL);
            methodVisitor.visitLabel(l1);
            methodVisitor.visitVarInsn(ASTORE, index);
        }
    }

    /**
     * This class generates byte code to divide 2 floats.
     */
    class PrivateDivideExpressionExecutorFloatBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((DivideExpressionExecutorFloat) conditionExecutor).getLeftExpressionExecutor();
            ExpressionExecutor right = ((DivideExpressionExecutorFloat) conditionExecutor).getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            Label l1 = new Label();

            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "floatValue", "()F",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) left).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2F);
                } else if (constantVariable instanceof Long) {
                    methodVisitor.visitInsn(L2F);
                }
            }

            methodVisitor.visitVarInsn(FSTORE, index);

            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "floatValue", "()F",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) right).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2F);
                } else if (constantVariable instanceof Long) {
                    methodVisitor.visitInsn(L2F);
                }
            }

            methodVisitor.visitVarInsn(FSTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index + 1);
            methodVisitor.visitInsn(FCONST_0);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFEQ, l0);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitVarInsn(FLOAD, index + 1);
            methodVisitor.visitInsn(FDIV);
            methodVisitor.visitMethodInsn(INVOKESTATIC, "java/lang/Float", "valueOf",
                    "(F)Ljava/lang/Float;", false);
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ACONST_NULL);
            methodVisitor.visitLabel(l1);
            methodVisitor.visitVarInsn(ASTORE, index);
        }
    }

    /**
     * This class generates byte code to divide 2 Integers.
     */
    class PrivateDivideExpressionExecutorIntegerBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((DivideExpressionExecutorInt) conditionExecutor).getLeftExpressionExecutor();
            ExpressionExecutor right = ((DivideExpressionExecutorInt) conditionExecutor).getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            Label l1 = new Label();

            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "intValue", "()I",
                        false);
            }

            methodVisitor.visitVarInsn(ISTORE, index);

            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "intValue", "()I",
                        false);
            }

            methodVisitor.visitVarInsn(ISTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index + 1);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitJumpInsn(IF_ICMPEQ, l0);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 1);
            methodVisitor.visitInsn(IDIV);
            methodVisitor.visitMethodInsn(INVOKESTATIC, "java/lang/Integer", "valueOf",
                    "(I)Ljava/lang/Integer;", false);
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ACONST_NULL);
            methodVisitor.visitLabel(l1);
            methodVisitor.visitVarInsn(ASTORE, index);
        }
    }

    /**
     * This class generates byte code to divide 2 long values.
     */
    class PrivateDivideExpressionExecutorLongBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((DivideExpressionExecutorLong) conditionExecutor).getLeftExpressionExecutor();
            ExpressionExecutor right = ((DivideExpressionExecutorLong) conditionExecutor).getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            Label l1 = new Label();

            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "longValue", "()J",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) left).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2L);
                }
            }

            methodVisitor.visitVarInsn(LSTORE, index);

            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "longValue", "()J",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) right).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2L);
                }
            }

            methodVisitor.visitVarInsn(LSTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index + 2);
            methodVisitor.visitInsn(LCONST_0);
            methodVisitor.visitInsn(LCMP);
            methodVisitor.visitJumpInsn(IFEQ, l0);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitVarInsn(LLOAD, index + 2);
            methodVisitor.visitInsn(LDIV);
            methodVisitor.visitMethodInsn(INVOKESTATIC, "java/lang/Long", "valueOf",
                    "(J)Ljava/lang/Long;", false);
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ACONST_NULL);
            methodVisitor.visitLabel(l1);
            methodVisitor.visitVarInsn(ASTORE, index);
        }
    }

    /**
     * This class generates byte code to mod 2 doubles.
     */
    class PrivateModExpressionExecutorDoubleBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((ModExpressionExecutorDouble) conditionExecutor).getLeftExpressionExecutor();
            ExpressionExecutor right = ((ModExpressionExecutorDouble) conditionExecutor).getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            Label l1 = new Label();

            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "doubleValue", "()D",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) left).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2D);
                } else if (constantVariable instanceof Float) {
                    methodVisitor.visitInsn(F2D);
                } else if (constantVariable instanceof Long) {
                    methodVisitor.visitInsn(L2D);
                }
            }

            methodVisitor.visitVarInsn(DSTORE, index);

            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "doubleValue", "()D",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) right).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2D);
                } else if (constantVariable instanceof Float) {
                    methodVisitor.visitInsn(F2D);
                } else if (constantVariable instanceof Long) {
                    methodVisitor.visitInsn(L2D);
                }
            }

            methodVisitor.visitVarInsn(DSTORE, index + 2);
            methodVisitor.visitVarInsn(DLOAD, index + 2);
            methodVisitor.visitInsn(DCONST_0);
            methodVisitor.visitInsn(DCMPL);
            methodVisitor.visitJumpInsn(IFEQ, l0);
            methodVisitor.visitVarInsn(DLOAD, index);
            methodVisitor.visitVarInsn(DLOAD, index + 2);
            methodVisitor.visitInsn(DREM);
            methodVisitor.visitMethodInsn(INVOKESTATIC, "java/lang/Double", "valueOf",
                    "(D)Ljava/lang/Double;", false);
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ACONST_NULL);
            methodVisitor.visitLabel(l1);
            methodVisitor.visitVarInsn(ASTORE, index);
        }
    }

    /**
     * This class generates byte code to mod 2 floats.
     */
    class PrivateModExpressionExecutorFloatBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((ModExpressionExecutorFloat) conditionExecutor).getLeftExpressionExecutor();
            ExpressionExecutor right = ((ModExpressionExecutorFloat) conditionExecutor).getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            Label l1 = new Label();

            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "floatValue", "()F",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) left).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2F);
                } else if (constantVariable instanceof Long) {
                    methodVisitor.visitInsn(L2F);
                }
            }

            methodVisitor.visitVarInsn(FSTORE, index);

            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "floatValue", "()F",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) right).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2F);
                } else if (constantVariable instanceof Long) {
                    methodVisitor.visitInsn(L2F);
                }
            }

            methodVisitor.visitVarInsn(FSTORE, index + 1);
            methodVisitor.visitVarInsn(FLOAD, index + 1);
            methodVisitor.visitInsn(FCONST_0);
            methodVisitor.visitInsn(FCMPL);
            methodVisitor.visitJumpInsn(IFEQ, l0);
            methodVisitor.visitVarInsn(FLOAD, index);
            methodVisitor.visitVarInsn(FLOAD, index + 1);
            methodVisitor.visitInsn(FREM);
            methodVisitor.visitMethodInsn(INVOKESTATIC, "java/lang/Float", "valueOf",
                    "(F)Ljava/lang/Float;", false);
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ACONST_NULL);
            methodVisitor.visitLabel(l1);
            methodVisitor.visitVarInsn(ASTORE, index);
        }
    }

    /**
     * This class generates byte code to mod 2 Integers.
     */
    class PrivateModExpressionExecutorIntegerBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((ModExpressionExecutorInt) conditionExecutor).getLeftExpressionExecutor();
            ExpressionExecutor right = ((ModExpressionExecutorInt) conditionExecutor).getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            Label l1 = new Label();

            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "intValue", "()I",
                        false);
            }

            methodVisitor.visitVarInsn(ISTORE, index);

            byteCodeGenerator.execute(right, index + 1, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 1);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "intValue", "()I",
                        false);
            }

            methodVisitor.visitVarInsn(ISTORE, index + 1);
            methodVisitor.visitVarInsn(ILOAD, index + 1);
            methodVisitor.visitInsn(ICONST_0);
            methodVisitor.visitJumpInsn(IF_ICMPEQ, l0);
            methodVisitor.visitVarInsn(ILOAD, index);
            methodVisitor.visitVarInsn(ILOAD, index + 1);
            methodVisitor.visitInsn(IREM);
            methodVisitor.visitMethodInsn(INVOKESTATIC, "java/lang/Integer", "valueOf",
                    "(I)Ljava/lang/Integer;", false);
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ACONST_NULL);
            methodVisitor.visitLabel(l1);
            methodVisitor.visitVarInsn(ASTORE, index);

        }
    }

    /**
     * This class generates byte code to mod 2 long values.
     */
    class PrivateModExpressionExecutorLongBytecodeEmitter implements ByteCodeEmitter {
        /**
         *
         * @param conditionExecutor
         * @param index
         * @param parent
         * @param specialCase
         * @param methodVisitor
         * @param byteCodeGenerator
         */
        @Override
        public void generate(ExpressionExecutor conditionExecutor, int index, int parent,
                             Label specialCase, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator) {
            ExpressionExecutor left = ((ModExpressionExecutorLong) conditionExecutor).getLeftExpressionExecutor();
            ExpressionExecutor right = ((ModExpressionExecutorLong) conditionExecutor).getRightExpressionExecutor();
            byteCodeGenerator.execute(left, index, 0, null, methodVisitor, byteCodeGenerator);
            Label l0 = new Label();
            Label l1 = new Label();

            if (!(left instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "longValue", "()J",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) left).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2L);
                }
            }

            methodVisitor.visitVarInsn(LSTORE, index);
            byteCodeGenerator.execute(right, index + 2, 0, null, methodVisitor, byteCodeGenerator);
            if (!(right instanceof ConstantExpressionExecutor)) {
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitJumpInsn(IFNULL, l0);
                methodVisitor.visitVarInsn(ALOAD, index + 2);
                methodVisitor.visitTypeInsn(CHECKCAST, "java/lang/Number");
                methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Number", "longValue", "()J",
                        false);
            } else {
                Object constantVariable = ((ConstantExpressionExecutor) right).getValue();
                if (constantVariable instanceof Integer) {
                    methodVisitor.visitInsn(I2L);
                }
            }

            methodVisitor.visitVarInsn(LSTORE, index + 2);
            methodVisitor.visitVarInsn(LLOAD, index + 2);
            methodVisitor.visitInsn(LCONST_0);
            methodVisitor.visitInsn(LCMP);
            methodVisitor.visitJumpInsn(IFEQ, l0);
            methodVisitor.visitVarInsn(LLOAD, index);
            methodVisitor.visitVarInsn(LLOAD, index + 2);
            methodVisitor.visitInsn(LREM);
            methodVisitor.visitMethodInsn(INVOKESTATIC, "java/lang/Long", "valueOf",
                    "(J)Ljava/lang/Long;", false);
            methodVisitor.visitJumpInsn(GOTO, l1);
            methodVisitor.visitLabel(l0);
            methodVisitor.visitInsn(ACONST_NULL);
            methodVisitor.visitLabel(l1);
            methodVisitor.visitVarInsn(ASTORE, index);
        }
    }
}
