/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/
package org.wso2.siddhi.classifiers.bayes;

import org.wso2.siddhi.classifiers.AbstractClassifier;
import org.wso2.siddhi.classifiers.OptionHandler;
import org.wso2.siddhi.classifiers.estimators.DiscreteEstimator;
import org.wso2.siddhi.classifiers.estimators.Estimator;
import org.wso2.siddhi.classifiers.trees.ht.Instance;
import org.wso2.siddhi.classifiers.trees.ht.Instances;
import org.wso2.siddhi.classifiers.trees.ht.utils.Utils;
import org.wso2.siddhi.classifiers.utils.Option;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.Enumeration;

public class NaiveBayes extends AbstractClassifier implements OptionHandler{

    protected int classCount;

    protected Instances instances;

    protected Estimator[][] distributions;

    protected Estimator classDistribution;

    @Override
    public void buildClassifier(Instances instances) throws Exception {

        // remove instances with missing class
        instances = new Instances(instances);
        instances.deleteWithMissingClass();

        classCount = instances.numClasses();

        // Copy the instances
        this.instances = new Instances(instances);

        // Reserve space for the distributions
        distributions = new Estimator[this.instances.numAttributes() - 1][this.instances
                .numClasses()];
        classDistribution = new DiscreteEstimator(this.instances.numClasses(), true);
        int attIndex = 0;
        Enumeration<Attribute> enu = this.instances.enumerateAttributes();
        while (enu.hasMoreElements()) {
            Attribute attribute = enu.nextElement();
            if (attribute.getType() == Attribute.Type.NUMERIC) {
                this.instances.sort(attribute);
                if ((this.instances.numInstances() > 0)
                        && !this.instances.instance(0).isMissing(attribute)) {
                    double lastVal = this.instances.instance(0).value(attribute);
                    double currentVal, deltaSum = 0;
                    int distinct = 0;
                    for (int i = 1; i < this.instances.numInstances(); i++) {
                        Instance currentInst = this.instances.instance(i);
                        if (currentInst.isMissing(attribute)) {
                            break;
                        }
                        currentVal = currentInst.value(attribute);
                        if (currentVal != lastVal) {
                            deltaSum += currentVal - lastVal;
                            lastVal = currentVal;
                            distinct++;
                        }
                    }
                }
            }

            for (int j = 0; j < this.instances.numClasses(); j++) {
                switch (attribute.getType()) {
                    case NOMINAL:
                        distributions[attIndex][j] = new DiscreteEstimator(
                                attribute.numValues(), true);
                        break;
                    default:
                        throw new Exception("Attribute type unknown to NaiveBayes");
                }
            }
            attIndex++;
        }

        // Compute counts
        Enumeration<Instance> enumInsts = this.instances.enumerateInstances();
        while (enumInsts.hasMoreElements()) {
            Instance instance = enumInsts.nextElement();
            updateClassifier(instance);
        }

        // Save space
        this.instances = new Instances(this.instances, 0);
    }

    public void updateClassifier(Instance instance) throws Exception {

        if (!instance.classIsMissing()) {
            Enumeration<Attribute> enumAtts = instances.enumerateAttributes();
            int attIndex = 0;
            while (enumAtts.hasMoreElements()) {
                Attribute attribute = enumAtts.nextElement();
                if (!instance.isMissing(attribute)) {
                    distributions[attIndex][(int) instance.classValue()].addValue(
                            instance.value(attribute), instance.weight());
                }
                attIndex++;
            }
            classDistribution.addValue(instance.classValue(), instance.weight());
        }
    }

    public double[] distributionForInstance(Instance instance) throws Exception {
        double[] probs = new double[classCount];
        for (int j = 0; j < classCount; j++) {
            probs[j] = classDistribution.getProbability(j);
        }
        Enumeration<Attribute> enumAtts = instance.enumerateAttributes();
        int attIndex = 0;
        while (enumAtts.hasMoreElements()) {
            Attribute attribute = enumAtts.nextElement();
            if (!instance.isMissing(attribute)) {
                double temp, max = 0;
                for (int j = 0; j < classCount; j++) {
                    temp = Math.max(1e-75, Math.pow(distributions[attIndex][j]
                                    .getProbability(instance.value(attribute)),
                            instances.attribute(attIndex).getM_Weight()));
                    probs[j] *= temp;
                    if (probs[j] > max) {
                        max = probs[j];
                    }
                    if (Double.isNaN(probs[j])) {
                        throw new Exception("NaN returned from estimator for attribute "
                                + attribute.getName() + ":\n"
                                + distributions[attIndex][j].toString());
                    }
                }
                if ((max > 0) && (max < 1e-75)) { // Danger of probability underflow
                    for (int j = 0; j < classCount; j++) {
                        probs[j] *= 1e75;
                    }
                }
            }
            attIndex++;
        }
        Utils.normalize(probs);
        return probs;
    }
    @Override
    public Enumeration<Option> listOptions() {
        return null;
    }

    @Override
    public void setOptions(String[] options) throws Exception {

    }

    @Override
    public String[] getOptions() {
        return new String[0];
    }
}
