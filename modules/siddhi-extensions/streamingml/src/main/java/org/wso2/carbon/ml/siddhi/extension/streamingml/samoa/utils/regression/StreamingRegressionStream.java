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

import com.github.javacliparser.IntOption;
import org.apache.samoa.instances.Attribute;
import org.apache.samoa.instances.DenseInstance;
import org.apache.samoa.instances.Instance;
import org.apache.samoa.instances.Instances;
import org.apache.samoa.instances.InstancesHeader;
import org.apache.samoa.moa.core.Example;
import org.apache.samoa.moa.core.InstanceExample;
import org.apache.samoa.moa.core.ObjectRepository;
import org.apache.samoa.moa.tasks.TaskMonitor;
import org.apache.samoa.streams.InstanceStream;
import org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils.DataStream;

import java.util.ArrayList;

public class StreamingRegressionStream extends DataStream {

    public IntOption numAttOption = new IntOption("numberOfAttributes", 'A',
            "The number of attributes in the stream.", 2, 1, Integer.MAX_VALUE);

    @Override
    protected void prepareForUseImpl(TaskMonitor taskMonitor, ObjectRepository objectRepository) {

        taskMonitor.setCurrentActivity("Preparing random RBF...", -1.0);
        this.numberOfAttributes = numAttOption.getValue();
        this.numberOfGeneratedInstances=0;
        generateHeader();
        restart();
        values = new double[numberOfAttributes];
        for (int i = 0; i < numberOfAttributes; i++) {
            values[i] = 0;
        }
    }

    protected void generateHeader() {
        ArrayList<Attribute> attributes = new ArrayList<Attribute>();
        for (int i = 0; i < numberOfAttributes; i++) {
            attributes.add(new Attribute("numeric" + (i + 1)));
        }
        streamHeader = new InstancesHeader(new Instances(getCLICreationString(InstanceStream.class),
                attributes, 0));
        streamHeader.setClassIndex(streamHeader.numAttributes() - 1);
    }

    @Override
    public Example<Instance> nextInstance() {
        double[] values_new = new double[numberOfAttributes];
        if (numberOfGeneratedInstances == 0) {
            while (cepEvents == null) ;
        }
        numberOfGeneratedInstances++;
        while (cepEvents.isEmpty()) ;
        double[] values = cepEvents.poll();
        System.arraycopy(values, 0, values_new, 0, values.length-1);
        Instance inst = new DenseInstance(1.0, values_new);
        inst.setDataset(getHeader());
        inst.setClassValue(values[values.length - 1]);// Set the relevant class value to the data set
        return new InstanceExample(inst);
    }



}
