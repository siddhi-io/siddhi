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

package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification;

import com.github.javacliparser.ClassOption;
import com.github.javacliparser.FlagOption;
import com.github.javacliparser.IntOption;
import com.github.javacliparser.Option;

import org.apache.samoa.tasks.Task;
import org.apache.samoa.topology.Topology;
import org.apache.samoa.topology.impl.SimpleComponentFactory;
import org.apache.samoa.topology.impl.SimpleEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;

import java.util.Queue;
import java.util.Vector;

public class StreamingClassificationTaskBuilder {


    // It seems that the 3 extra options are not used directly, But It used when convert sting to Task object.
    private static final String SUPPRESS_STATUS_OUT_MSG =
            "Suppress the task status output. Normally it is sent to stderr.";
    private static final String SUPPRESS_RESULT_OUT_MSG =
            "Suppress the task result output. Normally it is sent to stdout.";
    private static final String STATUS_UPDATE_FREQ_MSG =
            "Wait time in milliseconds between status updates.";
    private static final Logger logger =
            LoggerFactory.getLogger(StreamingClassificationTaskBuilder.class);


    public Queue<double[]> cepEvents;
    public Queue<Vector> classifiers;

    public int maxInstances;
    public int batchSize;
    public int numberOfClasses;
    public int numberOfAttributes;
    public int numberOfNominalAttributes;
    public int parallelism;
    public int bagging;

    Topology topology;

    public StreamingClassificationTaskBuilder(int maxInstance, int batchSize, int numClasses,
                                              int numAtts, int numNominals,
                                              Queue<double[]> cepEvents, Queue<Vector> classifiers,
                                              int par, int bag) {
        this.numberOfClasses = numClasses;
        this.cepEvents = cepEvents;
        this.maxInstances = maxInstance;
        this.numberOfAttributes = numAtts;
        this.numberOfNominalAttributes = numNominals;
        this.batchSize = batchSize;
        this.classifiers = classifiers;
        this.parallelism = par;
        this.bagging = bag;
    }

    public void initTask(String str) {
        String query;

        if (bagging == 0) {
            query = "org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification." +
                    "StreamingClassificationTask -f " + batchSize + " -i " + maxInstances +
                    " -s (org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification." +
                    "StreamingClassificationStream -K " + numberOfClasses + " -A " +
                    numberOfAttributes + " -N " + numberOfNominalAttributes + " -Z " + str +
                    " ) -l (org.apache.samoa.learners.classifiers.trees.VerticalHoeffdingTree -p "
                    + parallelism + ")";
        } else {
            //---------Bagging--------------
            query = "org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification." +
                    "StreamingClassificationTask -f " + batchSize + " -i " + maxInstances +
                    " -s (org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.classification." +
                    "StreamingClassificationStream -K " + numberOfClasses + " -A " +
                    numberOfAttributes + " -N " + numberOfNominalAttributes + " -Z " + str +
                    " ) -l (classifiers.ensemble.Bagging -s " + bagging +
                    " -l (classifiers.trees.VerticalHoeffdingTree -p " + parallelism + "))";

        }
        logger.info("QUERY: " + query);
        String args[] = {query};
        this.initClassificationTask(args);
    }

    private void initClassificationTask(String[] args) {

        /// These variables are not directly used
        FlagOption suppressStatusOutOpt = new FlagOption("suppressStatusOut", 'S',
                SUPPRESS_STATUS_OUT_MSG);
        FlagOption suppressResultOutOpt = new FlagOption("suppressResultOut", 'R',
                SUPPRESS_RESULT_OUT_MSG);
        IntOption statusUpdateFreqOpt = new IntOption("statusUpdateFrequency", 'F',
                STATUS_UPDATE_FREQ_MSG, 1000, 0, Integer.MAX_VALUE);
        Option[] extraOptions = new Option[]{suppressStatusOutOpt, suppressResultOutOpt,
                statusUpdateFreqOpt};
        ///

        StringBuilder cliString = new StringBuilder();
        for (String arg : args) {
            cliString.append(" ").append(arg);
        }

        Task task;
        try {
            // Convert that query to a Object of a Task
            task = ClassOption.cliStringToObject(cliString.toString(), Task.class, extraOptions);
        } catch (Exception e) {
            throw new ExecutionPlanRuntimeException("Fail to initialize the task.", e);

        }

        if (task instanceof StreamingClassificationTask) {
            StreamingClassificationTask t = (StreamingClassificationTask) task;  // Cast that created task to streamingclassificationTask
            t.setCepEvents(this.cepEvents);                                      //link task events to cepEvents
            t.setSamoaClassifiers(this.classifiers);
            t.setNumClasses(this.numberOfClasses);
        } else {
            throw new ExecutionPlanRuntimeException("Check Task: " +
                    "It is not a StreamingClassificationTask ");
        }

        task.setFactory(new SimpleComponentFactory());
        task.init();
        logger.info("Successfully Initiated the StreamingClassificationTask");
        topology=task.getTopology();
    }

    public void submit(){
        SimpleEngine.submitTopology(topology);
    }
}



