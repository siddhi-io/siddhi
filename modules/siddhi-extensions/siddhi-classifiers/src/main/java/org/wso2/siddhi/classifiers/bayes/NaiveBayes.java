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
import org.wso2.siddhi.classifiers.trees.ht.Instance;
import org.wso2.siddhi.classifiers.trees.ht.Instances;
import org.wso2.siddhi.classifiers.utils.Option;

import java.util.Enumeration;

public class NaiveBayes extends AbstractClassifier implements OptionHandler{

    protected int m_NumClasses;

    protected Instances m_Instances;

    protected Estimator[][] m_Distributions;


    @Override
    public void buildClassifier(Instances instances) throws Exception {

        // remove instances with missing class
        instances = new Instances(instances);
        instances.deleteWithMissingClass();

        m_NumClasses = instances.numClasses();

        // Copy the instances
        m_Instances = new Instances(instances);

        // Reserve space for the distributions
        m_Distributions = new Estimator[m_Instances.numAttributes() - 1][m_Instances
                .numClasses()];
        m_ClassDistribution = new DiscreteEstimator(m_Instances.numClasses(), true);
        int attIndex = 0;
        Enumeration<Attribute> enu = m_Instances.enumerateAttributes();
        while (enu.hasMoreElements()) {
            Attribute attribute = enu.nextElement();

            // If the attribute is numeric, determine the estimator
            // numeric precision from differences between adjacent values
            double numPrecision = DEFAULT_NUM_PRECISION;
            if (attribute.type() == Attribute.NUMERIC) {
                m_Instances.sort(attribute);
                if ((m_Instances.numInstances() > 0)
                        && !m_Instances.instance(0).isMissing(attribute)) {
                    double lastVal = m_Instances.instance(0).value(attribute);
                    double currentVal, deltaSum = 0;
                    int distinct = 0;
                    for (int i = 1; i < m_Instances.numInstances(); i++) {
                        Instance currentInst = m_Instances.instance(i);
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
                    if (distinct > 0) {
                        numPrecision = deltaSum / distinct;
                    }
                }
            }

            for (int j = 0; j < m_Instances.numClasses(); j++) {
                switch (attribute.type()) {
                    case Attribute.NUMERIC:
                        if (m_UseKernelEstimator) {
                            m_Distributions[attIndex][j] = new KernelEstimator(numPrecision);
                        } else {
                            m_Distributions[attIndex][j] = new NormalEstimator(numPrecision);
                        }
                        break;
                    case Attribute.NOMINAL:
                        m_Distributions[attIndex][j] = new DiscreteEstimator(
                                attribute.numValues(), true);
                        break;
                    default:
                        throw new Exception("Attribute type unknown to NaiveBayes");
                }
            }
            attIndex++;
        }

        // Compute counts
        Enumeration<Instance> enumInsts = m_Instances.enumerateInstances();
        while (enumInsts.hasMoreElements()) {
            Instance instance = enumInsts.nextElement();
            updateClassifier(instance);
        }

        // Save space
        m_Instances = new Instances(m_Instances, 0);
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
