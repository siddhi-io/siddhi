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

package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils.regression;

import com.github.javacliparser.ClassOption;
import com.github.javacliparser.FlagOption;
import com.github.javacliparser.IntOption;
import com.github.javacliparser.Option;
import org.apache.samoa.tasks.Task;
import org.apache.samoa.topology.impl.SimpleComponentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils.TaskBuilder;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;

import java.util.Queue;
import java.util.Vector;

public class StreamingRegressionTaskBuilder extends TaskBuilder {

    private static final Logger logger =
            LoggerFactory.getLogger(StreamingRegressionTaskBuilder.class);
    public Queue<Vector> samoaPredictions;
    public int batchSize;
    public int parallelism;

    public StreamingRegressionTaskBuilder(int maxInstance, int batchSize, int numAtts,
                                          Queue<double[]> cepEvents, Queue<Vector> data, int parallel) {
        this.cepEvents = cepEvents;
        this.maxInstances = maxInstance;
        this.batchSize = batchSize;
        this.numberOfAttributes = numAtts;
        this.samoaPredictions = data;
        this.parallelism = parallel;
    }

    public void initTask() {
        String query = "";
        query = "org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils.regression." +
                "StreamingRegressionTask -f " + batchSize + " -i " + maxInstances +
                " -s (org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils." +
                "regression.StreamingRegressionStream -A " + numberOfAttributes + " ) " +
                "-l  (org.apache.samoa.learners.classifiers.rules.HorizontalAMRulesRegressor " +
                "-r 9 -p " + parallelism + ")";

        logger.info("QUERY: " + query);
        String args[] = {query};
        this.initRegressionTask(args);
    }

    private void initRegressionTask(String[] args) {
        /// No usage directly
        FlagOption suppressStatusOutOpt =
                new FlagOption("suppressStatusOut", 'S', SUPPRESS_STATUS_OUT_MSG);
        FlagOption suppressResultOutOpt =
                new FlagOption("suppressResultOut", 'R', SUPPRESS_RESULT_OUT_MSG);
        IntOption statusUpdateFreqOpt =
                new IntOption("statusUpdateFrequency", 'F', STATUS_UPDATE_FREQ_MSG, 1000, 0,
                        Integer.MAX_VALUE);

        Option[] extraOptions = new Option[]{suppressStatusOutOpt, suppressResultOutOpt,
                statusUpdateFreqOpt};
        ///

        StringBuilder cliString = new StringBuilder();
        for (String arg : args) {
            cliString.append(" ").append(arg);
        }
        logger.debug("Command line string = {}", cliString.toString());

        Task task;
        try {
            task = ClassOption.cliStringToObject(cliString.toString(), Task.class, extraOptions);
        } catch (Exception e) {
            throw new ExecutionPlanRuntimeException("Fail to initialize the task : ", e);
        }

        if (task instanceof StreamingRegressionTask) {
            StreamingRegressionTask t = (StreamingRegressionTask) task;
            t.setCepEvents(this.cepEvents);
            t.setSamoaData(this.samoaPredictions);
        } else {
            throw new ExecutionPlanRuntimeException("Check Task: Not a StreamingRegressionTask");
        }

        task.setFactory(new SimpleComponentFactory());
        logger.info("Successfully Initialized Component Factory");
        task.init();
        logger.info("Successfully Initiated the StreamingRegressionTask");
        topology = task.getTopology();
    }
}
