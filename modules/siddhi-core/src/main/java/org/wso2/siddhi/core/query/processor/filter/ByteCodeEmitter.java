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
import org.wso2.siddhi.core.executor.ExpressionExecutor;

/**
 * Interface to generate byte code.
 */
public interface ByteCodeEmitter {
    /**
     * Abstract method to generate byte code.
     *
     * @param conditionExecutor ExpressionExecutor that needs byte-code generation.
     * @param index             Local variable array index where data related to Expression Executor is stored.
     * @param parent            Used to indicate  ExpressionExecutor before Current ExpressionExecutor.
     *                          0 - all other ExpressionExecutors.
     *                          1 - and ExpressionExecutor.
     *                          2 - or ExpressionExecutor.
     *                          3 - not ExpressionExecutor.
     * @param specialCase       Used to optimize if two consecutive same kind of ExpressionExecutors are met.
     *                          Ex- In the case of 2 consecutive 'and' ExpressionExecutors, if first 'and' is false
     *                          then straight away will jump to instruction after second 'and' ExpressionExecutor.
     * @param methodVisitor     Instance of ASM library class 'MethodVisitor' that used to generate a method inside the
     *                          byte-code class.
     */
    public void generate(ExpressionExecutor conditionExecutor, int index, int parent, Label specialCase,
                         MethodVisitor methodVisitor, ByteCodeGenerator byteCodeGenerator);
}
