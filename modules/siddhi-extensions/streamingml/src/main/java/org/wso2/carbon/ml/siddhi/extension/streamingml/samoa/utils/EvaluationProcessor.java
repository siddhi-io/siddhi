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
package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils;

import org.apache.samoa.core.ContentEvent;
import org.apache.samoa.core.Processor;
import org.apache.samoa.moa.evaluation.LearningCurve;
import org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils.classification.StreamingClassificationEvaluationProcessor;

import java.io.PrintStream;

/**
 * Source : https://github.com/apache/incubator-samoa/blob/master/samoa-api/src/main/java/org/
 * apache/samoa/evaluation/EvaluatorProcessor.java
 */
public abstract class EvaluationProcessor implements Processor {

    private static final long serialVersionUID = -2778051819116753612L;

    protected transient PrintStream immediateResultStream;
    protected transient boolean firstDump;
    protected long totalCount;
    protected long experimentStart;
    protected long sampleStart;
    protected LearningCurve learningCurve;
    protected int processId;

    public abstract boolean process(ContentEvent event);

    public abstract Processor newProcessor(Processor p);

    public abstract void onCreate(int id);

    public String toString() {
        StringBuilder report = new StringBuilder();
        report.append(StreamingClassificationEvaluationProcessor.class.getCanonicalName());
        report.append("processId = ").append(this.processId);
        report.append('\n');
        if (this.learningCurve.numEntries() > 0) {
            report.append(this.learningCurve.toString());
            report.append('\n');
        }
        return report.toString();
    }
}
