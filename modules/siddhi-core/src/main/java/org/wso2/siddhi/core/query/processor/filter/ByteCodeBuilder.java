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

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;

import static org.objectweb.asm.Opcodes.ACC_PRIVATE;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ACC_SUPER;
import static org.objectweb.asm.Opcodes.ALOAD;
import static org.objectweb.asm.Opcodes.ARETURN;
import static org.objectweb.asm.Opcodes.CHECKCAST;
import static org.objectweb.asm.Opcodes.GETFIELD;
import static org.objectweb.asm.Opcodes.GETSTATIC;
import static org.objectweb.asm.Opcodes.GOTO;
import static org.objectweb.asm.Opcodes.ICONST_0;
import static org.objectweb.asm.Opcodes.IF_ICMPGE;
import static org.objectweb.asm.Opcodes.ILOAD;
import static org.objectweb.asm.Opcodes.INVOKEINTERFACE;
import static org.objectweb.asm.Opcodes.INVOKESPECIAL;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;
import static org.objectweb.asm.Opcodes.ISTORE;
import static org.objectweb.asm.Opcodes.POP;
import static org.objectweb.asm.Opcodes.PUTFIELD;
import static org.objectweb.asm.Opcodes.RETURN;


/**
 * This class will insert initial and ending instructions to byte-code.
 */
public class ByteCodeBuilder {

    /**
     * This method creates a class called "JITCompiledExpressionExecutor" using byte code.
     *this class's constructor will pass a list of ExpressionExecutors to field unHandledExpressionExecutors.
     * @param classWriter Instance of ASM library class 'ClassWriter' that used to generate the byte-code class.
     */
    public static void jitCompiledExpressionExecutorClassPopulator(ClassWriter classWriter) {
        classWriter.visit(52, ACC_PUBLIC + ACC_SUPER, "JITCompiledExpressionExecutor", null,
                "java/lang/Object", new String[] { "org/wso2/siddhi/core/executor/ExpressionExecutor" });
        classWriter.visitSource("JITCompiledExpressionExecutor.java", null);

        MethodVisitor methodVisitor = classWriter.visitMethod(0, "<init>", "(Ljava/util/List;)V",
                "(Ljava/util/List<Lorg/wso2/siddhi/core/executor/ExpressionExecutor;>;)V", null);
        methodVisitor.visitCode();
        methodVisitor.visitVarInsn(ALOAD, 0);
        methodVisitor.visitMethodInsn(INVOKESPECIAL, "java/lang/Object", "<init>", "()V",
                false);
        methodVisitor.visitVarInsn(ALOAD, 0);
        methodVisitor.visitVarInsn(ALOAD, 1);
        methodVisitor.visitFieldInsn(PUTFIELD, "JITCompiledExpressionExecutor", "unHandledExpressionExecutors",
                "Ljava/util/List;");
        methodVisitor.visitInsn(RETURN);
        methodVisitor.visitMaxs(2, 2);
        methodVisitor.visitEnd();
    }

    /**
     * This method will insert a new field to the class JITCompiledExpressionExecutor. This field will keep list of
     * ExpressionExecutors which are not handled through byte-code generation.
     * @param classWriter
     */
    public static void unHandledExpressionExecutorsFieldPopulator(ClassWriter classWriter) {
        FieldVisitor fieldVisitor = classWriter.visitField(ACC_PRIVATE, "unHandledExpressionExecutors",
                "Ljava/util/List;", "Ljava/util/List<Lorg/wso2/siddhi/core/executor/ExpressionExecutor;>;",
                null);
        fieldVisitor.visitEnd();
    }

    /**
     * This method will insert a new method called cloneExecutor to the class JITCompiledExpressionExecutor. The method
     * cloneExecutor will update all unHandledExpressionExecutors with 'key' and will return an instance of
     * JITCompiledExpressionExecutor.
     * @param classWriter
     */
    public static void cloneExecutorMethodPopulator(ClassWriter classWriter) {
        MethodVisitor methodVisitor = classWriter.visitMethod(ACC_PUBLIC, "cloneExecutor",
                "(Ljava/lang/String;)Lorg/wso2/siddhi/core/executor/ExpressionExecutor;", null,
                null);
        methodVisitor.visitCode();
        methodVisitor.visitInsn(ICONST_0);
        methodVisitor.visitVarInsn(ISTORE, 2);
        Label l0 = new Label();
        methodVisitor.visitLabel(l0);
        methodVisitor.visitVarInsn(ILOAD, 2);
        methodVisitor.visitVarInsn(ALOAD, 0);
        methodVisitor.visitFieldInsn(GETFIELD, "JITCompiledExpressionExecutor", "unHandledExpressionExecutors",
                "Ljava/util/List;");
        methodVisitor.visitMethodInsn(INVOKEINTERFACE, "java/util/List", "size", "()I", true);
        Label l1 = new Label();
        methodVisitor.visitJumpInsn(IF_ICMPGE, l1);
        methodVisitor.visitVarInsn(ALOAD, 0);
        methodVisitor.visitFieldInsn(GETFIELD, "JITCompiledExpressionExecutor", "unHandledExpressionExecutors",
                "Ljava/util/List;");
        methodVisitor.visitVarInsn(ILOAD, 2);
        methodVisitor.visitMethodInsn(INVOKEINTERFACE, "java/util/List", "get", "(I)Ljava/lang/Object;", true);
        methodVisitor.visitTypeInsn(CHECKCAST, "org/wso2/siddhi/core/executor/ExpressionExecutor");
        methodVisitor.visitVarInsn(ALOAD, 1);
        methodVisitor.visitMethodInsn(INVOKEINTERFACE, "org/wso2/siddhi/core/executor/ExpressionExecutor",
                "cloneExecutor", "(Ljava/lang/String;)Lorg/wso2/siddhi/core/executor/ExpressionExecutor;", true);
        methodVisitor.visitInsn(POP);
        methodVisitor.visitIincInsn(2, 1);
        methodVisitor.visitJumpInsn(GOTO, l0);
        methodVisitor.visitLabel(l1);
        methodVisitor.visitVarInsn(ALOAD, 0);
        methodVisitor.visitInsn(ARETURN);
        methodVisitor.visitMaxs(2, 3);
        methodVisitor.visitEnd();
    }

    /**
     * This static method is used to define the "getReturnType" method inside the class JITCompiledExpressionExecutor.
     * Although the execute method of JITCompiledExpressionExecutor will return an object, getReturnType method will
     * give execute method's return type as BOOL inorder to overcome issues arise in the constructor of the
     * FilterProcessor.
     * @param classWriter
     */
    public static void getReturnTypeMethodPopulator(ClassWriter classWriter) {
        MethodVisitor methodVisitor = classWriter.visitMethod(ACC_PUBLIC, "getReturnType",
                "()Lorg/wso2/siddhi/query/api/definition/Attribute$Type;", null, null);
        methodVisitor.visitCode();
        methodVisitor.visitFieldInsn(GETSTATIC, "org/wso2/siddhi/query/api/definition/Attribute$Type",
                "BOOL", "Lorg/wso2/siddhi/query/api/definition/Attribute$Type;");
        methodVisitor.visitInsn(ARETURN);
        methodVisitor.visitMaxs(1, 1);
        methodVisitor.visitEnd();
    }

    /**
     * This method will insert a new method called execute to the class JITCompiledExpressionExecutor. The execute
     * method will provide final result to drop an Event or not.
     * @param classWriter
     * @return
     */
    public static MethodVisitor executeMethodPopulator(ClassWriter classWriter) {
        MethodVisitor methodVisitor = classWriter.visitMethod(ACC_PUBLIC, "execute",
                "(Lorg/wso2/siddhi/core/event/ComplexEvent;)Ljava/lang/Object;",
                null, null);
        methodVisitor.visitInsn(ICONST_0);

        return methodVisitor;
    }

    /**
     * This method finishes byte code generation and creates an instance of "JITCompiledExpressionExecutor" class.
     *
     * @param classWriter   Instance of ASM library class 'ClassWriter' that used to generate the byte-code class.
     * @param methodVisitor Instance of ASM library class 'MethodVisitor' that used to generate the execute method
     *                      inside byte-code class.
     * @param byteCodeGenerator Instance of ByteCodeGenerator class.
     * @param expressionExecutor Expression Executor of filter query.
     */
    public static void end(ClassWriter classWriter, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator,
                           ExpressionExecutor expressionExecutor) {
        if (!(expressionExecutor instanceof VariableExpressionExecutor)) {
            methodVisitor.visitMethodInsn(INVOKESTATIC, "java/lang/Boolean", "valueOf",
                    "(Z)Ljava/lang/Boolean;", false);
        } else {
            methodVisitor.visitVarInsn(ALOAD, 2);
        }

        methodVisitor.visitInsn(ARETURN);
        methodVisitor.visitMaxs(4, 12);
        methodVisitor.visitEnd();
        classWriter.visitEnd();
        byteCodeGenerator.setByteArray(classWriter.toByteArray());
        OptimizedExpressionExecutorClassLoader optimizedExpressionExecutorClassLoader =
                (OptimizedExpressionExecutorClassLoader)
                        AccessController.doPrivileged(new PrivilegedAction() {
                            public Object run() {
                                return (new OptimizedExpressionExecutorClassLoader()); //Instantiates the ClassLoader
                            }
                        });
        Class regeneratedClass = optimizedExpressionExecutorClassLoader
                .defineClass("JITCompiledExpressionExecutor", byteCodeGenerator.getByteArray());
        try {
            Constructor constructor = regeneratedClass.getDeclaredConstructor(List.class);
            constructor.setAccessible(true);
            byteCodeGenerator.setExpressionExecutor((ExpressionExecutor) constructor.
                    newInstance(byteCodeGenerator.getUnHandledExpressionExecutors()));
        } catch (InstantiationException e) {
            throw new SiddhiAppRuntimeException("Error while instantiating JITCompiledExpressionExecutor.", e);
        } catch (IllegalAccessException e) {
            throw new SiddhiAppRuntimeException("Error while instantiating JITCompiledExpressionExecutor.", e);
        } catch (NoSuchMethodException e) {
            throw new SiddhiAppRuntimeException("Error while finding constructor of JITCompiledExpressionExecutor.", e);
        } catch (InvocationTargetException e) {
            throw new SiddhiAppRuntimeException("Error while invoking constructor of JITCompiledExpressionExecutor.",
                    e);
        }
    }
}
