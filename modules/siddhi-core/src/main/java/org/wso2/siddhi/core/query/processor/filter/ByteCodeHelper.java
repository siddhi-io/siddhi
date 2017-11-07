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
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;

import static org.objectweb.asm.Opcodes.ACC_PRIVATE;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ACC_SUPER;
import static org.objectweb.asm.Opcodes.ALOAD;
import static org.objectweb.asm.Opcodes.ARETURN;
import static org.objectweb.asm.Opcodes.ICONST_0;
import static org.objectweb.asm.Opcodes.INVOKESPECIAL;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;
import static org.objectweb.asm.Opcodes.PUTFIELD;
import static org.objectweb.asm.Opcodes.RETURN;


/**
 * This class creates "ClassWriter" and Instantiates an object of class written using byte code.
 */
public class ByteCodeHelper {

    /**
     * This method creates a class called "ByteCodeRegistry" using byte code.
     *
     * @param classWriter Instance of ASM library class 'ClassWriter' that used to generate the byte-code class.
     * @return Instance of ASM library class 'MethodVisitor' that used to generate the execute method inside byte-code
     * class.
     */
    public MethodVisitor start(ClassWriter classWriter) {
        MethodVisitor methodVisitor;
        FieldVisitor fieldVisitor;
        classWriter.visit(52, ACC_PUBLIC + ACC_SUPER, "JITCompiledExpressionExecutor", null,
                "java/lang/Object", new String[] { "org/wso2/siddhi/core/executor/ExpressionExecutor" });
        classWriter.visitSource("JITCompiledExpressionExecutor.java", null);

        fieldVisitor = classWriter.visitField(ACC_PRIVATE, "unHandledExpressionExecutors",
                "Ljava/util/ArrayList;",
                "Ljava/util/ArrayList<Lorg/wso2/siddhi/core/executor/ExpressionExecutor;>;", null);
        fieldVisitor.visitEnd();

        methodVisitor = classWriter.visitMethod(0, "<init>", "(Ljava/util/ArrayList;)V",
                "(Ljava/util/ArrayList<Lorg/wso2/siddhi/core/executor/ExpressionExecutor;>;)V",
                null);
        methodVisitor.visitCode();
        methodVisitor.visitVarInsn(ALOAD, 0);
        methodVisitor.visitMethodInsn(INVOKESPECIAL, "java/lang/Object", "<init>", "()V",
                false);
        methodVisitor.visitVarInsn(ALOAD, 0);
        methodVisitor.visitVarInsn(ALOAD, 1);
        methodVisitor.visitFieldInsn(PUTFIELD, "JITCompiledExpressionExecutor",
                "unHandledExpressionExecutors", "Ljava/util/ArrayList;");
        methodVisitor.visitInsn(RETURN);
        methodVisitor.visitMaxs(2, 2);
        methodVisitor.visitEnd();

        methodVisitor = classWriter.visitMethod(ACC_PUBLIC, "execute",
                "(Lorg/wso2/siddhi/core/event/ComplexEvent;)Ljava/lang/Object;",
                null, null);
        methodVisitor.visitInsn(ICONST_0);

        return methodVisitor;
    }

    /**
     * This method finishes byte code generation and creates an instance of "ByteCodeRegistry" class.
     *
     * @param classWriter   Instance of ASM library class 'ClassWriter' that used to generate the byte-code class.
     * @param methodVisitor Instance of ASM library class 'MethodVisitor' that used to generate the execute method
     *                      inside byte-code class.
     * @param byteCodeGenerator Instance of ByteCodeGenerator class.
     * @param expressionExecutor Expression Executor of filter query.
     */
    public void end(ClassWriter classWriter, MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator,
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
        byteCodeGenerator.byteArray = classWriter.toByteArray();
        OptimizedExpressionExecutorClassLoader optimizedExpressionExecutorClassLoader =
                (OptimizedExpressionExecutorClassLoader)
                        AccessController.doPrivileged(new PrivilegedAction() {
                            public Object run() {
                                return (new OptimizedExpressionExecutorClassLoader()); //Instantiates the ClassLoader
                            }
                        });
        Class regeneratedClass = optimizedExpressionExecutorClassLoader
                .defineClass("JITCompiledExpressionExecutor", byteCodeGenerator.byteArray);
        try {
            Constructor constructor = regeneratedClass.getDeclaredConstructor(ArrayList.class);
            constructor.setAccessible(true);
            byteCodeGenerator.expressionExecutor = (ExpressionExecutor) constructor.
                    newInstance(byteCodeGenerator.unHandledExpressionExecutors);
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
