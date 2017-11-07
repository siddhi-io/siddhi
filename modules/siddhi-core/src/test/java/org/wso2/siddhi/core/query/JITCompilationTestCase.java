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
package org.wso2.siddhi.core.query;

import org.apache.log4j.Logger;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.testng.AssertJUnit;
import org.testng.TestNG;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.filter.ByteCodeGenerator;
import org.wso2.siddhi.core.query.processor.filter.ByteCodeHelper;
import org.wso2.siddhi.core.query.processor.filter.OptimizedExpressionExecutorClassLoader;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;

import static org.objectweb.asm.Opcodes.ARETURN;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;
import static org.wso2.siddhi.query.api.definition.Attribute.Type.BOOL;

public class JITCompilationTestCase {
    private static final Logger log = Logger.getLogger(JITCompilationTestCase.class);
    private volatile boolean defaultQueryJITCompile;

    @Test
    public void testForStartInByteCodeHelper() {
        log.info("Test for start() method in ByteCodeHelper.");
        ByteCodeHelper byteCodeHelper = new ByteCodeHelper();
        ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
        MethodVisitor methodVisitor = byteCodeHelper.start(classWriter);
        methodVisitor.visitMethodInsn(INVOKESTATIC, "java/lang/Boolean", "valueOf",
                "(Z)Ljava/lang/Boolean;", false);
        methodVisitor.visitInsn(ARETURN);
        methodVisitor.visitMaxs(1, 1);
        methodVisitor.visitEnd();
        classWriter.visitEnd();
        byte[] bytes = classWriter.toByteArray();
        OptimizedExpressionExecutorClassLoader optimizedExpressionExecutorClassLoader =
                (OptimizedExpressionExecutorClassLoader)
                        AccessController.doPrivileged(new PrivilegedAction() {
                            public Object run() {
                                return (new OptimizedExpressionExecutorClassLoader()); //Instantiates the ClassLoader
                            }
                        });

        log.info("Executing: Expression Executor.");
        Class regeneratedClass = optimizedExpressionExecutorClassLoader.defineClass("JITCompiledExpressionExecutor",
                bytes);
        ExpressionExecutor expressionExecutor = null;
        ArrayList<ExpressionExecutor> unHandledExpressionExecutors = new ArrayList<ExpressionExecutor>();
        try {
            Constructor constructor = regeneratedClass.getDeclaredConstructor(ArrayList.class);
            constructor.setAccessible(true);
            expressionExecutor = (ExpressionExecutor) constructor.
                    newInstance(unHandledExpressionExecutors);
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
        AssertJUnit.assertEquals(false, expressionExecutor.execute(null));

        AssertJUnit.assertEquals("java.lang.Object",
                regeneratedClass.getDeclaredMethods()[0].getGenericReturnType().getTypeName());
        AssertJUnit.assertEquals("org.wso2.siddhi.core.event.ComplexEvent",
                regeneratedClass.getDeclaredMethods()[0].getGenericParameterTypes()[0].getTypeName());

        AssertJUnit.assertEquals("unHandledExpressionExecutors", regeneratedClass.getDeclaredFields()[0].getName());
        AssertJUnit.assertEquals("java.util.ArrayList<org.wso2.siddhi.core.executor.ExpressionExecutor>",
                regeneratedClass.getDeclaredFields()[0].getGenericType().getTypeName());
    }

    @Test
    public void testForBuildInByteCodeGenerator() {
        log.info("Test for build() method in ByteCodeGenerator.");
        ByteCodeGenerator byteCodeGenerator = new ByteCodeGenerator();
        Attribute.Type type = BOOL;
        Attribute attribute = new Attribute("status", type);
        VariableExpressionExecutor variableExpressionExecutor = new VariableExpressionExecutor(attribute, 0, 0);
        int[] position = new int[]{-1, -1, 0, 0};
        variableExpressionExecutor.setPosition(position);
        ExpressionExecutor expressionExecutor = byteCodeGenerator.build(variableExpressionExecutor);
        StreamEvent streamEvent = new StreamEvent(1, 0, 0);
        streamEvent.setBeforeWindowData(true, 0);
        log.info("Executing: Expression Executor.");
        AssertJUnit.assertEquals(true, expressionExecutor.execute(streamEvent));
    }

    @Test
    public void rerunSiddhiFilterTestSuite() {
        this.defaultQueryJITCompile = Boolean.parseBoolean(System.getProperty("QueryJITCompile"));
        if (!(this.defaultQueryJITCompile)) {
            System.setProperty("QueryJITCompile", "true");
        } else {
            System.setProperty("QueryJITCompile", "false");
        }
        log.info("Changed the filter mode");
        Class[] filterTestClasses = {BooleanCompareTestCase.class, FilterTestCase1.class, FilterTestCase2.class,
                IsNullTestCase.class, SimpleQueryValidatorTestCase.class, StringCompareTestCase.class};
        TestNG testNG = new TestNG();
        testNG.setTestClasses(filterTestClasses);
        log.info("Starting to rerun Siddhi-Filter test cases.");
        testNG.run();
        System.setProperty("QueryJITCompile", String.valueOf(this.defaultQueryJITCompile));
        log.info("Reset the filter mode");
        if (testNG.hasFailure()) {
            AssertJUnit.fail("There are some errors in filter test cases for second mode.");
        }
    }
}
