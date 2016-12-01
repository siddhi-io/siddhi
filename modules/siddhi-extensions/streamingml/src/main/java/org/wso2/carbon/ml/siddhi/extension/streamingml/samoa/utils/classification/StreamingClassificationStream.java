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

package org.wso2.carbon.ml.siddhi.extension.streamingml.samoa.utils.classification;

import com.github.javacliparser.IntOption;
import com.github.javacliparser.StringOption;
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
import java.util.List;

public class StreamingClassificationStream extends DataStream {

    public IntOption numberOfClassesOption = new IntOption("numberOfClasses", 'K',
            "The number of classes in the model.", 2, 2, Integer.MAX_VALUE);
    public IntOption numberOfAttributesOption = new IntOption("numberOfAttributes", 'A',
            "The number of classes in the model.", 2, 1, Integer.MAX_VALUE);
    public IntOption numberOfNominalsOption = new IntOption("numberOfNominals", 'N',
            "The number of nominal attributes to generate.", 0, 0, 2147483647);
    public StringOption numberOfValuesPerNominalOption = new StringOption("numberOfValuesPerNominal" +
            "Option", 'Z', "The number of values per nominal attributes", "null");

    private int numberOfNominals;
    private int numberOfClasses;
    List<Integer> valuesForNominals;

    @Override
    protected void prepareForUseImpl(TaskMonitor taskMonitor, ObjectRepository objectRepository) {
        taskMonitor.setCurrentActivity("Preparing random RBF...", -1.0);

        valuesForNominals = new ArrayList<Integer>();
        this.numberOfClasses = numberOfClassesOption.getValue();
        this.numberOfAttributes = numberOfAttributesOption.getValue();
        this.numberOfNominals = numberOfNominalsOption.getValue();
        if (numberOfNominals != 0) {
            String[] valsForNominal = numberOfValuesPerNominalOption.getValue().split(",");
            for (String i : valsForNominal) {
                valuesForNominals.add(Integer.parseInt(i));
            }
        }
        generateHeader();
        restart();
        values = new double[numberOfAttributes];
        for (int i = 0; i < numberOfAttributes; i++) {
            values[i] = 0;
        }
    }

    protected void generateHeader() {
        List<Attribute> attributes = new ArrayList<Attribute>();

        // Add numerical values
        for (int i = 0; i < numberOfAttributes - 1 - numberOfNominalsOption.getValue(); i++) {
            attributes.add(new Attribute("numeric" + (i + 1)));
        }

        // Add nominal values
        for (int i = 0; i < this.numberOfNominalsOption.getValue(); ++i) {
            attributes.add(new Attribute("nominal" + (i + 1), getNominalAttributeValues(i)));
        }

        // Add class value
        ArrayList<String> classLabels = new ArrayList<String>();
        for (int i = 0; i < this.numberOfClasses; i++) {
            classLabels.add("class" + (i + 1));
        }

        attributes.add(new Attribute("class", classLabels));
        streamHeader = new InstancesHeader(new Instances(getCLICreationString(InstanceStream.class),
                attributes, 0));
        streamHeader.setClassIndex(streamHeader.numAttributes() - 1);
    }

    // Get number of values each nominal attribute has
    private ArrayList<String> getNominalAttributeValues(int count) {
        ArrayList<String> nominalAttValls = new ArrayList<>();
        for (int i = 0; i < valuesForNominals.get(count); ++i) {
            nominalAttValls.add("value" + (i + 1));
        }
        return nominalAttValls;
    }

    @Override
    public Example<Instance> nextInstance() {
        double[] valuesNew = new double[numberOfAttributes];
        if (numberOfGeneratedInstances == 0) {
            while (cepEvents == null) ;
        }
        numberOfGeneratedInstances++;
        while (cepEvents.isEmpty()) ;
        double[] values = cepEvents.poll();
        System.arraycopy(values, 0, valuesNew, 0, values.length - 1);
        Instance instance = new DenseInstance(1.0, valuesNew);
        instance.setDataset(getHeader());
        instance.setClassValue(values[values.length - 1]);// Set the relevant class value to the dataset
        return new InstanceExample(instance);
    }
}
